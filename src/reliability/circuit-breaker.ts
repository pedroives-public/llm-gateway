import { emitCbStateChange } from "../observability/events.js";

const FAILURE_WINDOW_MS = 30_000;
const FAILURE_LIMIT = 5;
const OPEN_COOLDOWN_MS = 60_000;

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";
export type ProbeOutcome = "SUCCESS" | "FAILURE";
export type RequestOutcome = ProbeOutcome | "INCONCLUSIVE";
export type ProbeVote = (outcome: ProbeOutcome) => void;

export interface CircuitAdmission {
  kind: "NORMAL" | "FAST_FAIL";
}

// Minimal logger the breaker needs; structurally compatible with events.ts.
export type CbLogger = {
  info: (obj: object) => void;
};

export interface CircuitBreaker {
  getState(): CircuitState;
  tryAcquire(): CircuitAdmission;
  recordResult(outcome: RequestOutcome): void;
}

export function createCircuitBreaker(
  log: CbLogger,
  runProbe?: (vote: ProbeVote) => void,
): CircuitBreaker {
  // Append-only failure instants (epoch ms). Read only via liveFailureStamps,
  // so the 30s window has one definition and the open-decision and event
  // snapshot can't drift.
  let failureTimeStamps: number[] = [];
  let state: CircuitState = "CLOSED";

  // The live 30s window, derived on read (expired excluded). Stamps arrive in
  // order and filter preserves it, so [0] is the oldest in-window failure.
  const liveFailureStamps = (now: number): number[] =>
    failureTimeStamps.filter((t) => now - t <= FAILURE_WINDOW_MS);

  // Emit cb_state_change via the shared events helper. The snapshot is the live
  // window at now, so recovery transitions (60s+ later) report an empty 0 / 0.
  const emitStateChange = (from: CircuitState, to: CircuitState): void => {
    const now = Date.now();
    const live = liveFailureStamps(now);
    emitCbStateChange(log, {
      from,
      to,
      failure_count: live.length,
      window_start_ms: live[0] ?? 0,
    });
  };

  const breaker: CircuitBreaker = {
    recordResult(outcome) {
      switch (state) {
        case "CLOSED":
          if (outcome === "FAILURE") {
            const now = Date.now();
            failureTimeStamps = [...liveFailureStamps(now), now];

            if (failureTimeStamps.length >= FAILURE_LIMIT) {
              state = "OPEN";
              armProbeTimer();
              emitStateChange("CLOSED", "OPEN");
            }
          }
          return;

        case "HALF_OPEN":
          // Tenant outcomes cannot transition or release the active probe.
          return;

        case "OPEN":
          // While OPEN we fast-fail without calling upstream — nothing to record.
          return;
      }
    },

    getState() {
      return state;
    },

    tryAcquire() {
      if (state === "CLOSED") return { kind: "NORMAL" };
      return { kind: "FAST_FAIL" };
    },
  };

  const recordProbeResult = (outcome: ProbeOutcome): void => {
    if (outcome === "SUCCESS") {
      state = "CLOSED";
      emitStateChange("HALF_OPEN", "CLOSED");
    } else {
      state = "OPEN";
      armProbeTimer();
      emitStateChange("HALF_OPEN", "OPEN");
    }
  };

  // Armed only on entry to OPEN, and OPEN has no other exit, so one attempt
  // runs at a time. The timer alone decides the 60s: a wall-clock check could
  // refuse the timer's own firing and leave OPEN with nothing armed.
  const armProbeTimer = (): void => {
    if (runProbe === undefined) return;

    const timer = setTimeout(() => {
      state = "HALF_OPEN";
      emitStateChange("OPEN", "HALF_OPEN");

      let voted = false;
      runProbe((outcome) => {
        if (voted) return;

        voted = true;
        recordProbeResult(outcome);
      });
    }, OPEN_COOLDOWN_MS);

    timer.unref();
  };

  return breaker;
}
