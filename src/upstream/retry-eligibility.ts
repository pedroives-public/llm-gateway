import type { ErrorOutcome, Outcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";
import { isInsufficientQuota } from "./quota.js";

const RETRY_AFTER_HONORED_STATUSES: readonly number[] = [429, 503];

export function isRetryEligible(outcome: ErrorOutcome): boolean {
  switch (outcome.kind) {
    case "upstream_error":
      // Quota exhaustion cannot clear within a retry window; staying
      // ineligible also keeps the Retry-After budget-skip passthrough
      // unreachable for quota, so its body never reaches the consumer.
      if (isInsufficientQuota(outcome)) {
        return false;
      }
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

// The Retry-After wait in ms if usable, else null — the single source for the
// retry sleep and the disposition label, so they can't disagree. Only 429/503
// carry temporal authority (RFC 6585 §4; RFC 9110 §10.2.3): other statuses
// jitter, keeping budget-skip unreachable. Positive delta-seconds only; absent
// or non-numeric (e.g. an HTTP-date) returns null.
export function retryAfterMs(outcome: Outcome): number | null {
  if (outcome.kind !== "upstream_error") {
    return null;
  }

  if (!RETRY_AFTER_HONORED_STATUSES.includes(outcome.status)) {
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
