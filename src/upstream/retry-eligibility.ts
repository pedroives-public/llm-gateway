import type { ErrorOutcome, Outcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";

export function isRetryEligible(outcome: ErrorOutcome): boolean {
  switch (outcome.kind) {
    case "upstream_error":
      return outcome.status >= 500 || outcome.status === 429;
    case "undecodable":
      return false;
    case "network_failed":
      return outcome.pre_send_proven;
    case "aborted":
      return false;
    default:
      return assertNever(outcome);
  }
}

// The Retry-After wait in ms if usable, else null — the single source both the
// retry sleep and the disposition label read, so they can't disagree on what
// counts as a Retry-After. Only positive delta-seconds qualify; absent or
// non-numeric (e.g. an HTTP-date) returns null.
export function retryAfterMs(outcome: Outcome): number | null {
  if (outcome.kind !== "upstream_error") {
    return null;
  }
  if (outcome.retry_after === undefined) {
    return null;
  }
  const seconds = Number(outcome.retry_after);
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return null;
  }
  return seconds * 1000;
}
