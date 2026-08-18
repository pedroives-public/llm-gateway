// Episode detector for auth DB unavailability. A failure sequence that
// crosses `threshold` inside the trailing `windowMs` is an episode the
// operator must hear about; an isolated failure is not. Callers feed every
// failure, unfiltered — false-positive protection lives in the threshold.
//
// State is in-memory only (persisting it in Postgres would be circular: the
// DB is the patient) and changes only on recorded events — quiet time neither
// opens nor closes. Only recovery evidence closes an episode; the next
// crossing is a new episode that alerts again, so the caller must emit
// OUTSIDE the once-per-process alert dedup: the closed→open transition is
// this alert's only dedup. Contract: at-least-once per episode (a duplicate
// after a restart is accepted noise).

// Compact only when the pruned prefix is both large and the majority of the
// array: reclaims memory without churning on every prune.
const COMPACTION_MIN_PRUNED = 1_024;

export interface AuthDbEpisodeOptions {
  /** Failure count inside the trailing window that opens an episode. */
  threshold: number;
  /** Trailing window length, in milliseconds. */
  windowMs: number;
}

export interface AuthDbEpisodeDetector {
  /**
   * Record one failure at `nowMs`. Returns true exactly when this failure
   * opens an episode — the caller emits the alert on that return, nowhere else.
   */
  recordFailure(nowMs: number): boolean;
  /** Recovery evidence (successful lookup): closes an open episode, if any. */
  recordSuccess(): void;
}

export function createAuthDbEpisodeDetector(
  options: AuthDbEpisodeOptions,
): AuthDbEpisodeDetector {
  let failures: number[] = [];
  let firstFailureIdx = 0;
  let isEpisodeOpen = false;

  return {
    recordFailure(nowMs: number): boolean {
      const windowStart = nowMs - options.windowMs;
      while (firstFailureIdx < failures.length) {
        const timestamp = failures[firstFailureIdx];
        if (timestamp === undefined || timestamp >= windowStart) {
          break;
        }
        firstFailureIdx++;
      }
      failures.push(nowMs);

      const failuresInWindow = failures.length - firstFailureIdx;
      if (failuresInWindow >= options.threshold) {
        if (!isEpisodeOpen) {
          isEpisodeOpen = true;
          return true;
        }
      }

      if (
        firstFailureIdx > COMPACTION_MIN_PRUNED &&
        firstFailureIdx * 2 > failures.length
      ) {
        failures = failures.slice(firstFailureIdx);
        firstFailureIdx = 0;
      }

      return false;
    },
    recordSuccess(): void {
      // No reset while closed: a flapping DB (failures interleaved with
      // successes) must still cross the threshold. Closing clears the window,
      // so a re-alert needs a fresh crossing.
      if (!isEpisodeOpen) {
        return;
      }
      isEpisodeOpen = false;
      failures = [];
      firstFailureIdx = 0;
    },
  };
}
