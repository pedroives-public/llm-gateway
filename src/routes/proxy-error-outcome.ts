import type { ErrorOutcome } from "../upstream/outcome.js";
import type { ErrorClass, RetryDisposition } from "../observability/events.js";
import type { Classification } from "../upstream/classify.js";
import { assertNever } from "../upstream/assert-never.js";
import {
  isRetryEligible,
  retryAfterMs,
} from "../upstream/retry-eligibility.js";
import { isAcceptedStream, type AttemptResult } from "../upstream/stream.js";

// How an error outcome resolves on the buffered path: the error class and
// breaker vote compose with the retry disposition into a status and a response
// body. Pure functions over outcomes; nothing here touches the reply or the
// breaker. The two sanitized bodies live here because both branches of the
// route answer with them.

export const TOTAL_TIMEOUT_BODY = {
  error: {
    message: "stream exceeded the total duration limit",
    type: "gateway_timeout",
    code: "total_timeout_exceeded",
  },
};
export const UPSTREAM_DECODE_ERROR_BODY = {
  error: {
    message: "invalid response from upstream",
    type: "server_error",
    code: "upstream_decode_error",
  },
};

// Reconstructs WHY a request did or did not retry from what the retry
// primitive leaves behind (attempts, outcome, signal). Callers pass a real
// upstream outcome (attempts >= 1); the breaker-OPEN fast-fail sets
// `ineligible` directly. Exported so a unit test can pin the no-Retry-After
// budget-skip, a path unreachable end-to-end.
export function deriveRetryDisposition(
  attempts: number,
  outcome: AttemptResult,
  signal: AbortSignal,
): RetryDisposition {
  if (attempts === 2) {
    return "attempted";
  }

  if (isAcceptedStream(outcome) || outcome.kind === "ok") {
    return "ineligible";
  }

  if (
    isRetryEligible(outcome) &&
    !signal.aborted &&
    retryAfterMs(outcome) !== null
  ) {
    return "skipped_budget";
  }
  return "ineligible";
}

// What an error outcome resolves to — the one place the per-outcome
// classification and the per-episode disposition compose.
type ErrorTerminal = {
  error_class: ErrorClass;
  breaker_delta: 0 | 1;
  status: number;
};

// Suppress an upstream budget-skip: the upstream asked us to back off, so it
// is not a breaker-worthy fault. Only a 429/503 can reach this branch —
// skipped_budget requires a usable Retry-After, and retryAfterMs grants that
// authority to those statuses alone (non-upstream outcomes never qualify);
// the kind guard narrows the type and keeps impossible kinds falling through.
export function decideErrorTerminal(
  classification: Classification,
  disposition: RetryDisposition,
  outcome: ErrorOutcome,
): ErrorTerminal {
  if (disposition === "skipped_budget" && outcome.kind === "upstream_error") {
    return {
      ...classification,
      breaker_delta: 0,
      status: outcome.status,
    };
  }

  return {
    error_class: classification.error_class,
    breaker_delta: classification.breaker_delta,
    status: statusForErrorOutcome(outcome, classification.error_class),
  };
}

// For an upstream_error terminal: does this class answer with the
// deterministic sanitized 502 (true) or pass the upstream's own status and
// body through verbatim (false)? Total over ErrorClass: adding a class
// without deciding here is a compile error.
export function isSanitizedUpstreamTerminal(errorClass: ErrorClass): boolean {
  switch (errorClass) {
    // Operator-culpable rejections: upstream body/headers never reach the consumer.
    case "upstream-auth-failure":
    case "upstream-access-denied":
    case "upstream-quota-exhausted":
      return true;
    // Consumer-relevant upstream responses pass through verbatim.
    case "client-fault":
    case "upstream-retry-exhausted":
      return false;
    // Never produced from an upstream_error outcome today; if that ever
    // changes, sanitize by default — no unvetted body reaches the consumer.
    case "gateway-fault":
    case "upstream-fault":
    case "upstream-redirect-blocked":
      return true;

    default:
      assertNever(errorClass);
  }
}

function statusForErrorOutcome(
  outcome: ErrorOutcome,
  errorClass: ErrorClass,
): number {
  switch (outcome.kind) {
    case "upstream_error":
      // Sanitized terminals are proxy-boundary failures: the upstream was
      // reached but refused the deployment's own account context — 502,
      // never the upstream's original status.
      if (isSanitizedUpstreamTerminal(errorClass)) {
        return 502;
      }
      return outcome.status >= 500 ? 502 : outcome.status;
    case "undecodable":
      return 502;
    // Proxy-boundary failure on the operator's side (stale endpoint config or
    // an upstream that started redirecting): 502, never a passthrough.
    case "redirect_blocked":
      return 502;
    case "network_failed":
      return errorClass === "gateway-fault" ? 504 : 502;
    case "aborted":
      switch (outcome.abort_kind) {
        case "wall_clock_expired":
        case "total_timeout":
          return 504;
        case "response_size_cap":
          return 502;
        default:
          return assertNever(outcome.abort_kind);
      }
  }
}

export function bodyForErrorOutcome(
  outcome: ErrorOutcome,
  errorClass: ErrorClass,
): unknown {
  switch (outcome.kind) {
    case "upstream_error":
      // Only sanitized terminal classes reach here; verbatim passthrough
      // returns earlier in the handler.
      if (errorClass === "upstream-auth-failure") {
        return {
          error: {
            message: "upstream authentication failed",
            type: "server_error",
            code: "upstream_auth_failure",
          },
        };
      }
      if (errorClass === "upstream-access-denied") {
        return {
          error: {
            message: "upstream access denied",
            type: "server_error",
            code: "upstream_access_denied",
          },
        };
      }
      if (errorClass === "upstream-quota-exhausted") {
        return {
          error: {
            message: "upstream quota exhausted",
            type: "server_error",
            code: "upstream_quota_exhausted",
          },
        };
      }
      return outcome.body_raw;

    case "undecodable":
      return UPSTREAM_DECODE_ERROR_BODY;

    case "redirect_blocked":
      return {
        error: {
          message: "upstream redirect blocked",
          type: "server_error",
          code: "upstream_redirect_blocked",
        },
      };

    case "network_failed":
      if (errorClass === "gateway-fault") {
        return {
          error: {
            message: "gateway error",
            type: "gateway_error",
            code: "upstream_connection_failed",
          },
        };
      }

      return {
        error: {
          message: "upstream unavailable",
          type: "server_error",
          code: "upstream_unavailable",
        },
      };

    case "aborted":
      switch (outcome.abort_kind) {
        case "response_size_cap":
          return {
            error: {
              message: "upstream response too large",
              type: "server_error",
              code: "response_too_large",
            },
          };
        case "wall_clock_expired":
          return {
            error: {
              message: "gateway timeout",
              type: "gateway_timeout",
              code: "wall_clock_exceeded",
            },
          };
        // The handler ends the streaming deadline's own abort as TOTAL_TIMEOUT
        // before it reaches these switches, so this arm and the one in
        // statusForErrorOutcome are unreachable today. They keep the switches
        // exhaustive and give the same answer.
        case "total_timeout":
          return TOTAL_TIMEOUT_BODY;
        default:
          return assertNever(outcome.abort_kind);
      }

    default:
      return assertNever(outcome);
  }
}
