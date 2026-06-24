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
} from "../observability/events.js";
import { retry } from "../reliability/retry.js";
import { armWallClockTimeout } from "../reliability/timeouts.js";
import { classify } from "../upstream/classify.js";
import { assertNever } from "../upstream/assert-never.js";

const WALL_CLOCK_MS = 30_000;
const BODY_LIMIT_BYTES = 262_144;

export interface ChatCompletionsBody {
  model: string;
  messages: unknown[];
  stream?: boolean;
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
    stream: { type: "boolean" },
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
          duration_ms: durationMs,
          upstream_duration_ms: 0,
          gateway_overhead_ms: durationMs,
          attempts: 0,
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
          duration_ms: durationMs,
          upstream_duration_ms: upstreamDurationMs,
          gateway_overhead_ms: gatewayOverheadMs,
          attempts,
        });

        reply.code(outcome.status);
        return outcome.body_parsed;
      }

      const classification = classify(outcome, request.log, request.reqId);
      opts.breaker.recordResult(
        classification.breaker_delta === 1 ? "FAILURE" : "INCONCLUSIVE",
      );

      const status = statusForErrorOutcome(outcome, classification.error_class);
      emitReqComplete(request.log, {
        req_id: request.reqId,
        status,
        error_class: classification.error_class,
        duration_ms: durationMs,
        upstream_duration_ms: upstreamDurationMs,
        gateway_overhead_ms: gatewayOverheadMs,
        attempts,
      });

      reply
        .code(status)
        .header("x-gateway-error-class", classification.error_class);

      if (outcome.kind === "upstream_error") {
        if (outcome.retry_after !== undefined) {
          reply.header("retry-after", outcome.retry_after);
        }
        return outcome.body_raw;
      }

      return bodyForErrorOutcome(outcome, classification.error_class);
    },
  );
};

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

// Branch selection: `error.validation` is set only for schema rejections (a 413
// leaves it undefined); an explicit 413 status is the body-too-large case;
// anything else is unhandled.
function sendProxyError(
  error: FastifyError,
  request: FastifyRequest,
  reply: FastifyReply,
): void {
  if (error.validation && error.validation.length > 0) {
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: error.message,
          type: "invalid_request_error",
          code: deriveValidationCode(error.validation),
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

function deriveValidationCode(
  validation: FastifySchemaValidationError[],
): string {
  switch (validation[0]?.keyword) {
    case "required":
      return `${String(validation[0].params.missingProperty)}_missing`;
    case "minItems":
      return `${validation[0].instancePath.slice(1)}_empty`;
    default:
      return "invalid_request";
  }
}
