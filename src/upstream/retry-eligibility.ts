import type { ErrorOutcome } from "./outcome.js";
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
