import { PROBE_BACKSTOP_MS } from "../config.js";
import {
  emitBreakerProbeBackstop,
  emitBreakerProbeResult,
  emitCbStateChange,
  type ProbeOutcome,
} from "../observability/events.js";

export type { ProbeOutcome } from "../observability/events.js";

const FAILURE_WINDOW_MS = 30_000;
const FAILURE_LIMIT = 5;
const OPEN_COOLDOWN_MS = 60_000;

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";
export type RequestOutcome = ProbeOutcome | "INCONCLUSIVE";
export type ProbeVote = (outcome: ProbeOutcome) => void;

export interface CircuitAdmission {
  kind: "NORMAL" | "FAST_FAIL";
}

// Minimal logger the breaker needs; structurally compatible with events.ts.
// `error` carries the lines that only a gateway defect produces.
export type CbLogger = {
  info: (obj: object) => void;
  error: (obj: object) => void;
};

export interface CircuitBreaker {
  getState(): CircuitState;
  tryAcquire(): CircuitAdmission;
  recordResult(outcome: RequestOutcome): void;
  close(): void;
}

export function createCircuitBreaker(
  log: CbLogger,
  runProbe?: (vote: ProbeVote, signal: AbortSignal) => void | Promise<void>,
): CircuitBreaker {
  // Append-only failure instants (epoch ms). Read only via liveFailureStamps,
  // so the 30s window has one definition and the open-decision and event
  // snapshot can't drift.
  let failureTimeStamps: number[] = [];
  let state: CircuitState = "CLOSED";
  let probeAttempts = 0;

  let isClosed = false;
  let probeTimer: ReturnType<typeof setTimeout> | undefined;
  let probeBackstopTimer: ReturnType<typeof setTimeout> | undefined;
  let probeController: AbortController | undefined;

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

    close() {
      isClosed = true;

      if (probeTimer !== undefined) {
        clearTimeout(probeTimer);
        probeTimer = undefined;
      }

      if (probeController !== undefined) {
        probeController.abort();
        probeController = undefined;
      }

      if (probeBackstopTimer !== undefined) {
        clearTimeout(probeBackstopTimer);
        probeBackstopTimer = undefined;
      }
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
    if (isClosed || runProbe === undefined) return;

    const timer = setTimeout(() => {
      state = "HALF_OPEN";
      try {
        emitStateChange("OPEN", "HALF_OPEN");
      } catch {
        // State-change logging is observational. If it fails before the probe
        // starts, restore liveness by returning to OPEN and arming the next
        // timer.
        state = "OPEN";
        armProbeTimer();
        return;
      }

      const controller = new AbortController();
      probeController = controller;

      const attempt = ++probeAttempts;
      let voted = false;
      let attemptBackstopTimer: ReturnType<typeof setTimeout> | undefined;

      const clearAttemptBackstop = (): void => {
        if (attemptBackstopTimer === undefined) return;

        clearTimeout(attemptBackstopTimer);

        if (probeBackstopTimer === attemptBackstopTimer) {
          probeBackstopTimer = undefined;
        }

        attemptBackstopTimer = undefined;
      };

      const voteOnce: ProbeVote = (outcome) => {
        if (isClosed || voted) return;
        voted = true;
        clearAttemptBackstop();

        try {
          recordProbeResult(outcome);
        } catch {
          // A state-change log failure must not skip the probe-result event.
        }
        try {
          emitBreakerProbeResult(log, { attempt, outcome });
        } catch {
          // Probe votes do not propagate logging failures to their caller.
        }
      };

      const failProbe = (): void => {
        voteOnce("FAILURE");
      };

      const backstopTimer = setTimeout(() => {
        if (isClosed || voted) return;

        if (probeBackstopTimer === backstopTimer) {
          probeBackstopTimer = undefined;
        }
        attemptBackstopTimer = undefined;

        failProbe();
        controller.abort();

        try {
          emitBreakerProbeBackstop(log, { attempt });
        } catch {
          // A logging failure must not undo a backstop vote.
        }
      }, PROBE_BACKSTOP_MS);

      attemptBackstopTimer = backstopTimer;
      probeBackstopTimer = backstopTimer;
      backstopTimer.unref();

      try {
        const result = runProbe(voteOnce, controller.signal);
        if (result !== undefined) {
          void Promise.resolve(result).then(
            () => {
              if (!voted) failProbe();
            },
            failProbe,
          );
        }
      } catch {
        failProbe();
      }
    }, OPEN_COOLDOWN_MS);

    probeTimer = timer;
    timer.unref();
  };

  return breaker;
}
