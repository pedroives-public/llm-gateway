import type { Outcome } from "./outcome.js";

// Quota-exhaustion recognition for upstream 429 bodies — the single source
// both retry eligibility and classification read (same pattern as
// retryAfterMs). Strict match, fail-open: an unparseable or unexpected body
// is a legitimate rate-limit, never quota.
// EVIDENCE DEBT: the shape is documented-only; promote the first real quota
// 429 body captured in staging/production to a test fixture.
export function isInsufficientQuota(outcome: Outcome): boolean {
  if (outcome.kind !== "upstream_error") return false;
  if (outcome.status !== 429) return false;

  let bodyParsed: unknown;
  try {
    bodyParsed = JSON.parse(outcome.body_raw);
  } catch {
    return false;
  }

  if (typeof bodyParsed !== "object" || bodyParsed === null) {
    return false;
  }

  if (!("error" in bodyParsed)) {
    return false;
  }

  const error: unknown = bodyParsed.error;

  if (typeof error !== "object" || error === null) {
    return false;
  }

  if (!("code" in error)) {
    return false;
  }

  const code: unknown = error.code;
  return code === "insufficient_quota";
}
