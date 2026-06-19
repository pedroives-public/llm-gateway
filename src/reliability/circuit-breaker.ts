import { emitCbStateChange } from "../observability/events.js";

const FAILURE_WINDOW_MS = 30_000;
const FAILURE_LIMIT = 5;
const OPEN_COOLDOWN_MS = 60_000;

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";
export type ProbeOutcome = "SUCCESS" | "FAILURE" | "INCONCLUSIVE";

export interface CircuitAdmission {
  kind: "NORMAL" | "PROBE" | "FAST_FAIL";
}

// Minimal logger the breaker needs; structurally compatible with events.ts.
export type CbLogger = {
  info: (obj: object) => void;
};

export interface CircuitBreaker {
  getState(): CircuitState;
  tryAcquire(): CircuitAdmission;
  recordResult(outcome: ProbeOutcome): void;
}

export function createCircuitBreaker(log: CbLogger): CircuitBreaker {
  // Append-only failure instants (epoch ms). Read only via liveFailureStamps,
  // so the 30s window has one definition and the open-decision and event
  // snapshot can't drift.
  let failureTimeStamps: number[] = [];
  let state: CircuitState = "CLOSED";
  let openedAt: number | null = null;
  let probeInFlight = false;

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

  return {
    recordResult(outcome) {
      switch (state) {
        case "CLOSED":
          if (outcome === "FAILURE") {
            const now = Date.now();
            failureTimeStamps = [...liveFailureStamps(now), now];

            if (failureTimeStamps.length >= FAILURE_LIMIT) {
              state = "OPEN";
              openedAt = now;
              emitStateChange("CLOSED", "OPEN");
            }
          }
          return;

        case "HALF_OPEN":
          try {
            if (outcome === "SUCCESS") {
              state = "CLOSED";
              emitStateChange("HALF_OPEN", "CLOSED");
            } else if (outcome === "FAILURE") {
              state = "OPEN";
              openedAt = Date.now();
              emitStateChange("HALF_OPEN", "OPEN");
            }
          } finally {
            // Free the probe slot on EVERY terminal — success, re-open,
            // inconclusive no-op, or a throw mid-record. In finally, not the
            // branches, so a probe can never wedge the breaker HALF_OPEN.
            probeInFlight = false;
          }
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
      const now = Date.now();

      if (
        state === "OPEN" &&
        openedAt !== null &&
        now - openedAt >= OPEN_COOLDOWN_MS
      ) {
        state = "HALF_OPEN";
        emitStateChange("OPEN", "HALF_OPEN");
      }

      if (state === "CLOSED") return { kind: "NORMAL" };
      if (state === "OPEN") return { kind: "FAST_FAIL" };
      if (probeInFlight) return { kind: "FAST_FAIL" };

      probeInFlight = true;
      return { kind: "PROBE" };
    },
  };
}
