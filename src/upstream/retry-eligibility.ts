import type { ErrorOutcome, Outcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";

const RETRY_AFTER_HONORED_STATUSES: readonly number[] = [429, 503];

export function isRetryEligible(outcome: ErrorOutcome): boolean {
  switch (outcome.kind) {
    case "upstream_error":
      // Default-deny: an upstream error status cannot prove the attempt did
      // not cross the inference commit point (acceptance/start of execution,
      // with its quota consumption and billing). Eligibility returns only via
      // a per-provider adapter contract; the per-status derivation is pinned
      // in retry-eligibility.test.ts. Quota exhaustion rides on this blanket
      // deny — a 429 reopening must reinstate its carve-out, and the
      // insufficient_quota test cell goes red if forgotten.
      return false;
    case "undecodable":
      return false;
    case "network_failed":
      return outcome.pre_send_proven;
    // A blocked redirect cannot clear within the retry budget: the upstream
    // answers 3xx again until the deployment endpoint config changes.
    case "redirect_blocked":
      return false;
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
