import type { ErrorClass } from "../observability/events.js";

export type Outcome =
  | {
      kind: "http_status";
      status: number;
    }
  | {
      kind: "decode_error";
    }
  | {
      kind: "connect_error";
    };

export type Classification = {
  error_class: ErrorClass;
  retry_eligible: boolean;
  breaker_delta: 0 | 1;
};

function assertNever(value: never): never {
  throw new Error(`Unexpected value: ${JSON.stringify(value)}`);
}

export function classify(outcome: Outcome): Classification {
  switch (outcome.kind) {
    case "http_status":
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
    case "decode_error":
      return {
        error_class: "upstream-fault",
        retry_eligible: false,
        breaker_delta: 1,
      };
    case "connect_error":
      return {
        error_class: "upstream-retry-exhausted",
        retry_eligible: true,
        breaker_delta: 1,
      };
    default:
      return assertNever(outcome);
  }
}
