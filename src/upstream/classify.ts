import type { ErrorClass } from "../observability/events.js";
import type { ErrorOutcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";
import { isInsufficientQuota } from "./quota.js";

export type CauseLogPayload = {
  req_id: string;
  cause_code: string | null;
  cause_name: string | null;
};

export type MinLogger = {
  error: (obj: CauseLogPayload) => void;
};

export type Classification = {
  error_class: ErrorClass;
  breaker_delta: 0 | 1;
};

function logUpstreamCause(
  outcome: ErrorOutcome,
  log: MinLogger,
  reqId: string,
): void {
  switch (outcome.kind) {
    // A blocked redirect is upstream-sourced (a 3xx answer) with no transport
    // cause to record; it keeps the one-cause-line-per-upstream-failure join.
    case "redirect_blocked":
    case "upstream_error":
    case "undecodable":
      log.error({ req_id: reqId, cause_code: null, cause_name: null });
      return;
    case "network_failed":
      log.error({
        req_id: reqId,
        cause_code: outcome.cause_code ?? null,
        cause_name: outcome.cause_name ?? null,
      });
      return;
    case "aborted":
      return;
    default:
      return assertNever(outcome);
  }
}

export function classify(
  outcome: ErrorOutcome,
  log: MinLogger,
  reqId: string,
): Classification {
  logUpstreamCause(outcome, log, reqId);
  switch (outcome.kind) {
    case "upstream_error":
      if (outcome.status >= 500) {
        return {
          error_class: "upstream-retry-exhausted",
          breaker_delta: 1,
        };
      }
      // Deployment-owned credential rejected upstream (consumer input cannot
      // reach the auth headers — pinned by the adapter header-isolation test):
      // a persistent operator-side failure, so it counts toward the breaker.
      if (outcome.status === 401) {
        return {
          error_class: "upstream-auth-failure",
          breaker_delta: 1,
        };
      }
      // Access denial is consumer-influencible (the free-form model field can
      // induce 403s at will), so it must not count toward the shared breaker.
      if (outcome.status === 403) {
        return {
          error_class: "upstream-access-denied",
          breaker_delta: 0,
        };
      }
      // Quota exhaustion is an operator-account condition recognized from
      // the response body; body-derived signals never count toward the
      // breaker (it tracks transport/availability faults only).
      if (isInsufficientQuota(outcome)) {
        return {
          error_class: "upstream-quota-exhausted",
          breaker_delta: 0,
        };
      }
      if (outcome.status === 429) {
        return {
          error_class: "upstream-retry-exhausted",
          breaker_delta: 0,
        };
      }
      return {
        error_class: "client-fault",
        breaker_delta: 0,
      };
    case "undecodable":
      return {
        error_class: "upstream-fault",
        breaker_delta: 1,
      };
    // The gateway's own redirect policy refused the upstream's 3xx: every
    // following call fails with certainty until the deployment endpoint
    // config changes, so it counts toward the breaker (fail fast for
    // callers) regardless of upstream health.
    case "redirect_blocked":
      return {
        error_class: "upstream-redirect-blocked",
        breaker_delta: 1,
      };
    case "network_failed":
      if (outcome.pre_send_proven) {
        return {
          error_class: "upstream-retry-exhausted",
          breaker_delta: 1,
        };
      }
      return {
        error_class: "gateway-fault",
        breaker_delta: 1,
      };
    case "aborted":
      switch (outcome.abort_kind) {
        case "response_size_cap":
          return {
            error_class: "upstream-fault",
            breaker_delta: 0,
          };
        case "wall_clock_expired":
          return {
            error_class: "gateway-fault",
            breaker_delta: 1,
          };
        default:
          return assertNever(outcome.abort_kind);
      }
    default:
      return assertNever(outcome);
  }
}
