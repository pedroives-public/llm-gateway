import type { ErrorClass } from "../observability/events.js";
import type { ErrorOutcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";

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
