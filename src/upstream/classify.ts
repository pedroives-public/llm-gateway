import type { ErrorClass } from "../observability/events.js";
import type { ErrorOutcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";

export type Classification = {
  error_class: ErrorClass;
  retry_eligible: boolean;
  breaker_delta: 0 | 1;
};

export function classify(outcome: ErrorOutcome): Classification {
  switch (outcome.kind) {
    case "upstream_error":
      if (outcome.status >= 500) {
        return {
          error_class: "upstream-retry-exhausted",
          retry_eligible: true,
          breaker_delta: 1,
        };
      }
      if (outcome.status === 429) {
        return {
          error_class: "upstream-retry-exhausted",
          retry_eligible: true,
          breaker_delta: 0,
        };
      }
      return {
        error_class: "client-fault",
        retry_eligible: false,
        breaker_delta: 0,
      };
    case "undecodable":
      return {
        error_class: "upstream-fault",
        retry_eligible: false,
        breaker_delta: 1,
      };
    case "network_failed":
      if (outcome.pre_send_proven) {
        return {
          error_class: "upstream-retry-exhausted",
          retry_eligible: true,
          breaker_delta: 1,
        };
      }
      return {
        error_class: "gateway-fault",
        retry_eligible: false,
        breaker_delta: 1,
      };
    case "aborted":
      switch (outcome.abort_kind) {
        case "response_size_cap":
          return {
            error_class: "upstream-fault",
            retry_eligible: false,
            breaker_delta: 0,
          };
        case "wall_clock_expired":
          return {
            error_class: "gateway-fault",
            retry_eligible: false,
            breaker_delta: 1,
          };
        default:
          return assertNever(outcome.abort_kind);
      }
    default:
      return assertNever(outcome);
  }
}
