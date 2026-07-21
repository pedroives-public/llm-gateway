import type {
  FastifyError,
  FastifyPluginAsync,
  FastifyReply,
  FastifyRequest,
  FastifySchemaValidationError,
} from "fastify";
import type { CircuitBreaker } from "../reliability/circuit-breaker.js";
import type { ErrorOutcome, Outcome } from "../upstream/outcome.js";
import type { Logger } from "../upstream/rejection.js";
import {
  emitReqStart,
  emitReqComplete,
  wasColdStart,
  type ErrorClass,
  type RetryDisposition,
  emitReqRejected,
  type ReqRejectedReason,
} from "../observability/events.js";
import { retry } from "../reliability/retry.js";
import { armWallClockTimeout } from "../reliability/timeouts.js";
import { classify, type Classification } from "../upstream/classify.js";
import { assertNever } from "../upstream/assert-never.js";
import {
  isRetryEligible,
  retryAfterMs,
} from "../upstream/retry-eligibility.js";

const WALL_CLOCK_MS = 30_000;
const BODY_LIMIT_BYTES = 262_144;

export interface ChatCompletionsBody {
  model: string;
  messages: unknown[];
  // Buffered-only V1: the schema rejects stream:true, so the type carries the
  // same pin; widen back to `boolean` when streaming ships.
  stream?: false;
  [key: string]: unknown;
}

// DI seam: app.ts injects the real breaker + OpenAI client; tests inject fakes.
// The client returns an Outcome, so it composes directly under retry().
export interface ProxyRouteOptions {
  breaker: CircuitBreaker;
  upstreamBuffered: (
    body: ChatCompletionsBody,
    signal: AbortSignal,
    log: Logger,
  ) => Promise<Outcome>;
}

const chatCompletionsBodySchema = {
  type: "object",
  required: ["model", "messages"],
  properties: {
    model: { type: "string" },
    messages: { type: "array", minItems: 1 },
    // Buffered-only V1: reject stream:true at the schema, before it can reach the breaker or upstream; lift when streaming ships.
    stream: { type: "boolean", const: false },
    // Cost cap: the upstream honors either field (max_completion_tokens succeeds max_tokens),
    // so both carry the same ceiling on purpose — capping only one leaves the other as a bypass.
    max_tokens: { type: "integer", minimum: 1, maximum: 16384 },
    max_completion_tokens: { type: "integer", minimum: 1, maximum: 16384 },
    // n (choice count) stays 1 in V1: each extra choice multiplies the output cost
    n: { type: "integer", const: 1 },
  },
  additionalProperties: true,
};

export const proxyRoute: FastifyPluginAsync<ProxyRouteOptions> = async (
  fastify,
  opts,
) => {
  // Error desk for Fastify-generated faults only. Upstream faults are shaped
  // inline by the handler and never reach here; the lone exception is the
  // re-thrown unrecognized upstream rejection, which lands as 500 gateway-fault.
  fastify.setErrorHandler<FastifyError>((error, request, reply) => {
    sendProxyError(error, request, reply);
  });

  fastify.post<{ Body: ChatCompletionsBody }>(
    "/v1/chat/completions",
    {
      bodyLimit: BODY_LIMIT_BYTES,
      schema: { body: chatCompletionsBodySchema },
    },
    async (request, reply) => {
      // Identity guard: missing tenant/plan is an auth misconfiguration, not a
      // client error — refuse before any upstream call.
      if (request.tenantId === null || request.planTier === null) {
        reply.code(500).header("x-gateway-error-class", "gateway-fault");
        return {
          error: {
            message: "internal server error",
            type: "internal_error",
            code: "unhandled_exception",
          },
        };
      }

      const wasCold = wasColdStart();

      emitReqStart(request.log, {
        req_id: request.reqId,
        route: "/v1/chat/completions",
        tenant_id: request.tenantId,
        plan_tier: request.planTier,
        stream: request.body.stream ?? false,
        idempotency_key_present:
          request.headers["idempotency-key"] !== undefined,
        was_cold_start: wasCold,
      });

      const requestStartedAt = Date.now();
      const admission = opts.breaker.tryAcquire();

      if (admission.kind === "FAST_FAIL") {
        const status = 503;
        const errorClass: ErrorClass = "upstream-retry-exhausted";
        const durationMs = Date.now() - requestStartedAt;

        emitReqComplete(request.log, {
          req_id: request.reqId,
          status,
          error_class: errorClass,
          stream: request.body.stream ?? false,
          duration_ms: durationMs,
          upstream_duration_ms: 0,
          gateway_overhead_ms: durationMs,
          attempts: 0,
          retry_disposition: "ineligible",
        });

        reply.code(status).header("x-gateway-error-class", errorClass);
        return {
          error: {
            message: "Service temporarily unavailable",
            type: "service_unavailable",
            code: "circuit_breaker_open",
          },
        };
      }

      const armedAt = Date.now();
      const timeout = armWallClockTimeout(WALL_CLOCK_MS);
      const deadlineAt = armedAt + WALL_CLOCK_MS;
      let attempts = 0;
      let upstreamDurationMs = 0;

      const upstreamLog = request.log.child({ req_id: request.reqId });
      const callUpstream = async (): Promise<Outcome> => {
        attempts += 1;
        const attemptStartedAt = Date.now();
        try {
          return await opts.upstreamBuffered(
            request.body,
            timeout.signal,
            upstreamLog,
          );
        } finally {
          upstreamDurationMs += Date.now() - attemptStartedAt;
        }
      };

      let outcome: Outcome;
      try {
        outcome = await retry(callUpstream, {
          signal: timeout.signal,
          deadlineAt,
          // Buffered-only V1: nothing is flushed before the terminal; replace
          // with the real first-byte marker when streaming ships.
          firstByteFlushed: () => false,
        });
      } catch (error) {
        opts.breaker.recordResult("INCONCLUSIVE");
        throw error;
      } finally {
        timeout.clear();
      }

      const durationMs = Date.now() - requestStartedAt;
      const gatewayOverheadMs = Math.max(0, durationMs - upstreamDurationMs);

      if (outcome.kind === "ok") {
        opts.breaker.recordResult("SUCCESS");
        emitReqComplete(request.log, {
          req_id: request.reqId,
          status: outcome.status,
          error_class: null,
          stream: request.body.stream ?? false,
          duration_ms: durationMs,
          upstream_duration_ms: upstreamDurationMs,
          gateway_overhead_ms: gatewayOverheadMs,
          attempts,
          retry_disposition: deriveRetryDisposition(
            attempts,
            outcome,
            timeout.signal,
          ),
        });

        reply.code(outcome.status);
        return outcome.body_parsed;
      }

      const classification = classify(outcome, request.log, request.reqId);
      const retryDisposition = deriveRetryDisposition(
        attempts,
        outcome,
        timeout.signal,
      );

      // recordResult uses the policy's delta, not classify's raw delta, in the
      // same synchronous stretch — no await between deciding and recording.
      const terminal = decideErrorTerminal(
        classification,
        retryDisposition,
        outcome,
      );
      opts.breaker.recordResult(
        terminal.breaker_delta === 1 ? "FAILURE" : "INCONCLUSIVE",
      );

      emitReqComplete(request.log, {
        req_id: request.reqId,
        status: terminal.status,
        error_class: terminal.error_class,
        stream: request.body.stream ?? false,
        duration_ms: durationMs,
        upstream_duration_ms: upstreamDurationMs,
        gateway_overhead_ms: gatewayOverheadMs,
        attempts,
        retry_disposition: retryDisposition,
      });

      reply
        .code(terminal.status)
        .header("x-gateway-error-class", terminal.error_class);

      if (outcome.kind === "upstream_error") {
        if (outcome.retry_after !== undefined) {
          reply.header("retry-after", outcome.retry_after);
        }
        return outcome.body_raw;
      }

      return bodyForErrorOutcome(outcome, terminal.error_class);
    },
  );
};

// Reconstructs WHY a request did or did not retry from what the retry
// primitive leaves behind (attempts, outcome, signal). Callers pass a real
// upstream outcome (attempts >= 1); the breaker-OPEN fast-fail sets
// `ineligible` directly. Exported so a unit test can pin the no-Retry-After
// budget-skip, a path unreachable end-to-end.
export function deriveRetryDisposition(
  attempts: number,
  outcome: Outcome,
  signal: AbortSignal,
): RetryDisposition {
  if (attempts === 2) {
    return "attempted";
  }

  if (outcome.kind === "ok") {
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

// Suppress ONLY an upstream 429/503 budget-skip: the upstream asked us to back
// off, so it is not a breaker-worthy fault. A budget-skipped network_failed
// (e.g. connect timeout) is a real failure and MUST fall through so the breaker
// counts it.
function decideErrorTerminal(
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

function statusForErrorOutcome(
  outcome: ErrorOutcome,
  errorClass: ErrorClass,
): number {
  switch (outcome.kind) {
    case "upstream_error":
      return outcome.status >= 500 ? 502 : outcome.status;
    case "undecodable":
      return 502;
    case "network_failed":
      return errorClass === "gateway-fault" ? 504 : 502;
    case "aborted":
      switch (outcome.abort_kind) {
        case "wall_clock_expired":
          return 504;
        case "response_size_cap":
          return 502;
        default:
          return assertNever(outcome.abort_kind);
      }
  }
}

function bodyForErrorOutcome(
  outcome: ErrorOutcome,
  errorClass: ErrorClass,
): unknown {
  switch (outcome.kind) {
    case "upstream_error":
      return outcome.body_raw;

    case "undecodable":
      return {
        error: {
          message: "invalid response from upstream",
          type: "server_error",
          code: "upstream_decode_error",
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
        default:
          return assertNever(outcome.abort_kind);
      }

    default:
      return assertNever(outcome);
  }
}

// Branch selection: a schema rejection requires BOTH the FST_ERR_VALIDATION
// code AND a populated `validation` array — a lone signal falls to the
// unhandled 500 branch. Parse failures match stable FST_ERR_CTP_* codes;
// explicit 413 is body-too-large; anything else is a gateway fault.
function sendProxyError(
  error: FastifyError,
  request: FastifyRequest,
  reply: FastifyReply,
): void {
  if (
    error.validation &&
    error.validation.length > 0 &&
    error.code === "FST_ERR_VALIDATION"
  ) {
    const { code, reason } = deriveValidationRejection(error.validation);

    if (request.tenantId === null) {
      request.log.error(
        { req_id: request.reqId, err_name: error.name },
        "schema rejection with null tenant: auth misconfiguration",
      );
    } else {
      emitReqRejected(request.log, {
        req_id: request.reqId,
        tenant_id: request.tenantId,
        route: request.routeOptions.url,
        reason,
        status: 400,
      });
    }
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: error.message,
          type: "invalid_request_error",
          code,
        },
      });

    return;
  }

  if (error.code === "FST_ERR_CTP_INVALID_JSON_BODY") {
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request body is not valid JSON",
          type: "invalid_request_error",
          code: "malformed_json",
        },
      });
    return;
  }

  if (error.code === "FST_ERR_CTP_EMPTY_JSON_BODY") {
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request body is empty",
          type: "invalid_request_error",
          code: "empty_body",
        },
      });
    return;
  }

  if (error.code === "FST_ERR_CTP_INVALID_MEDIA_TYPE") {
    reply
      .code(415)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request content type unsupported",
          type: "invalid_request_error",
          code: "unsupported_content_type",
        },
      });
    return;
  }

  if (error.statusCode === 413) {
    reply
      .code(413)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request body too large",
          type: "invalid_request_error",
          code: "request_too_large",
        },
      });
    return;
  }

  // Log only allowlisted fields — the raw error (stack/cause) must stay out of
  // both the client body and the log payload.
  request.log.error(
    { req_id: request.reqId, err_name: error.name },
    "unhandled exception in proxy handler",
  );
  reply
    .code(500)
    .header("x-gateway-error-class", "gateway-fault")
    .send({
      error: {
        message: "internal server error",
        type: "internal_error",
        code: "unhandled_exception",
      },
    });
}

function deriveValidationRejection(
  validation: FastifySchemaValidationError[],
): {
  code: string;
  reason: ReqRejectedReason;
} {
  // Both checks on purpose in each const branch: keyword alone would claim
  // any other `const` field; path alone would mislabel a type violation on
  // the same field (e.g. stream: "x").
  if (
    validation[0]?.keyword === "const" &&
    validation[0].instancePath === "/n"
  ) {
    return {
      code: "n_not_supported",
      reason: "cost_cap_exceeded",
    };
  }

  // Remove this branch when streaming ships.
  if (
    validation[0]?.keyword === "const" &&
    validation[0].instancePath === "/stream"
  ) {
    return {
      code: "stream_not_supported",
      reason: "schema_validation",
    };
  }

  switch (validation[0]?.keyword) {
    case "required":
      return {
        code: `${String(validation[0].params.missingProperty)}_missing`,
        reason: "schema_validation",
      };
    case "minItems":
      return {
        code: `${validation[0].instancePath.slice(1)}_empty`,
        reason: "schema_validation",
      };
    case "maximum":
      return {
        code: `${validation[0].instancePath.slice(1)}_too_large`,
        reason: "cost_cap_exceeded",
      };
    default:
      return { code: "invalid_request", reason: "schema_validation" };
  }
}
