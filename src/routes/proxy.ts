import type { FastifyError, FastifyPluginAsync } from "fastify";
import type { CircuitBreaker } from "../reliability/circuit-breaker.js";
import type { ErrorOutcome, Outcome } from "../upstream/outcome.js";
import { resolveRejection, type Logger } from "../upstream/rejection.js";
import {
  emitReqStart,
  emitReqComplete,
  wasColdStart,
  type ErrorClass,
  emitOperationalAlert,
  alertFor,
  type MinLogger,
  type ReqCompleteTerminal,
  emitStreamDone,
} from "../observability/events.js";
import { retry } from "../reliability/retry.js";
import {
  armWallClockTimeout,
  armTotalDurationTimeout,
} from "../reliability/timeouts.js";
import { classify } from "../upstream/classify.js";
import {
  isAcceptedStream,
  type AttemptResult,
  type StreamingAdapter,
  cancelStream,
  type AcceptedStream,
} from "../upstream/stream.js";
import {
  createSseFrameReader,
  type SseFrame,
} from "../upstream/sse-frame-reader.js";
import { STREAM_TOTAL_DURATION_MS } from "../config.js";
import {
  finishFirstFrameFault,
  finishTotalTimeout,
  isTotalTimeout,
} from "./proxy-first-frame.js";
import {
  bodyForErrorOutcome,
  decideErrorTerminal,
  deriveRetryDisposition,
  isSanitizedUpstreamTerminal,
} from "./proxy-error-outcome.js";
import { sendProxyError } from "./proxy-error-handler.js";

const WALL_CLOCK_MS = 30_000;
const BODY_LIMIT_BYTES = 262_144;
const MAX_OUTPUT_TOKENS_CAP = 16_384;

export interface ChatCompletionsBody {
  model: string;
  messages: unknown[];
  // `true` is reachable only when the streaming flag is ON; with the flag OFF
  // the schema rejects it before the handler runs.
  stream?: boolean;
  [key: string]: unknown;
}

// DI seam: app.ts injects the real breaker + OpenAI client; tests inject fakes.
// The client returns an Outcome, so it composes directly under retry().
export interface ProxyRouteOptions {
  breaker: CircuitBreaker;
  // Boot-time streaming flag, read once by config validation. It reaches the
  // route only through this option: the route never reads process.env, so no
  // request can influence which path it takes. Omitted means OFF, the same
  // fail-closed default as an unset environment variable.
  streamingEnabled?: boolean;
  upstreamBuffered: (
    body: ChatCompletionsBody,
    signal: AbortSignal,
    log: Logger,
  ) => Promise<Outcome>;
  upstreamStreaming?: (
    body: ChatCompletionsBody,
    signal: AbortSignal,
    log: Logger & MinLogger,
  ) => Promise<StreamingAdapter>;
}

const chatCompletionsBodySchema = {
  type: "object",
  required: ["model", "messages"],
  properties: {
    model: { type: "string", minLength: 1 },
    messages: { type: "array", minItems: 1 },
    // Flag-OFF form: reject stream:true at the schema, before it can reach the
    // breaker or the upstream. The flag-ON form drops the `const`.
    stream: { type: "boolean", const: false },
    // Cost cap: the upstream honors either field (max_completion_tokens succeeds max_tokens),
    // so both carry the same ceiling on purpose — capping only one leaves the other as a bypass.
    max_tokens: { type: "integer", minimum: 1, maximum: MAX_OUTPUT_TOKENS_CAP },
    max_completion_tokens: {
      type: "integer",
      minimum: 1,
      maximum: MAX_OUTPUT_TOKENS_CAP,
    },
    // n (choice count) stays 1 in V1: each extra choice multiplies the output cost
    n: { type: "integer", const: 1 },
  },
  additionalProperties: true,
};

// Called once per route registration, never per request. The ON form is a copy:
// the shared OFF schema is never mutated, so two apps in one process can hold
// different flag states. Overriding `stream` in place keeps it ahead of `n`,
// and that order is load-bearing: Ajv stops at the first violation and
// deriveValidationRejection reads only validation[0].
function buildChatCompletionsBodySchema(streamingEnabled: boolean) {
  if (streamingEnabled) {
    return {
      ...chatCompletionsBodySchema,
      properties: {
        ...chatCompletionsBodySchema.properties,
        stream: { type: "boolean" },
      },
    };
  }
  return chatCompletionsBodySchema;
}

function resolveStreamingUpstream(
  enabled: boolean,
  upstream: ProxyRouteOptions["upstreamStreaming"],
): NonNullable<ProxyRouteOptions["upstreamStreaming"]> {
  if (upstream !== undefined) {
    return upstream;
  }

  if (enabled) {
    throw new Error(
      "Streaming is enabled but no upstreamStreaming function was provided to the proxy route",
    );
  }

  return async () => {
    throw new Error(
      "Streaming was requested without the streaming flag enabled on the route",
    );
  };
}


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

  const streamingEnabled = opts.streamingEnabled ?? false;
  const streamingUpstream = resolveStreamingUpstream(
    streamingEnabled,
    opts.upstreamStreaming,
  );

  fastify.post<{ Body: ChatCompletionsBody }>(
    "/v1/chat/completions",
    {
      bodyLimit: BODY_LIMIT_BYTES,
      schema: { body: buildChatCompletionsBodySchema(streamingEnabled) },
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
      let terminalDecided = false;

      const timeout =
        request.body.stream === true
          ? armTotalDurationTimeout(
              armedAt + STREAM_TOTAL_DURATION_MS,
              () => terminalDecided,
            )
          : armWallClockTimeout(WALL_CLOCK_MS);
      const deadlineAt =
        request.body.stream === true
          ? armedAt + STREAM_TOTAL_DURATION_MS
          : armedAt + WALL_CLOCK_MS;
      let attempts = 0;
      let upstreamDurationMs = 0;

      // A schema maximum only fires on a present field: when the client omits
      // both spend fields, inject the cap so omission cannot reach the
      // upstream uncapped.
      const hasSpendField =
        request.body.max_tokens !== undefined ||
        request.body.max_completion_tokens !== undefined;

      const forwardedBody = hasSpendField
        ? request.body
        : { ...request.body, max_tokens: MAX_OUTPUT_TOKENS_CAP };

      const upstreamLog = request.log.child({ req_id: request.reqId });
      const callUpstream = async (): Promise<AttemptResult> => {
        attempts += 1;
        const attemptStartedAt = Date.now();

        const upstream =
          request.body.stream === true
            ? streamingUpstream
            : opts.upstreamBuffered;

        try {
          return await upstream(forwardedBody, timeout.signal, upstreamLog);
        } finally {
          upstreamDurationMs += Date.now() - attemptStartedAt;
        }
      };

      try {
        let outcome: AttemptResult;
        try {
          outcome = await retry(callUpstream, {
            signal: timeout.signal,
            deadlineAt,
            // No byte reaches the client before the terminal is decided: an
            // accepted stream is closed without forwarding any frame, so this
            // stays false until frames are forwarded.
            firstByteFlushed: () => false,
          });
        } catch (error) {
          terminalDecided = true;
          opts.breaker.recordResult("INCONCLUSIVE");
          throw error;
        }

        const durationMs = Date.now() - requestStartedAt;
        const gatewayOverheadMs = Math.max(0, durationMs - upstreamDurationMs);

        // The deadline ends the request when the re-read clock has reached it, or
        // when its own timer has already aborted: that timer aborts only after
        // reading the deadline value, so the reason on the signal records a
        // reading that held, even if the clock has stepped back since. The
        // reason is read, not the aborted flag, because the flag cannot tell the
        // deadline's abort from any other cause that comes to share this signal.
        if (isTotalTimeout(request.body.stream, deadlineAt, timeout.signal)) {
          terminalDecided = true;

          return finishTotalTimeout({
            acceptedStream: isAcceptedStream(outcome) ? outcome : undefined,
            outcome,
            attempts,
            requestStartedAt,
            upstreamDurationMs,
            request: {
              log: request.log,
              reqId: request.reqId,
            },
            reply,
            signal: timeout.signal,
            upstreamLog,
            breaker: opts.breaker,
          });
        }

        let acceptedStream: AcceptedStream | undefined;
        if (isAcceptedStream(outcome)) {
          acceptedStream = outcome;
          const nextFrame = createSseFrameReader(acceptedStream.reader);
          let firstFrame: SseFrame | undefined;
          let readError: ErrorOutcome | undefined;

          try {
            firstFrame = await nextFrame();
            while (firstFrame.kind === "frame" && firstFrame.data === null) {
              firstFrame = await nextFrame();
            }
          } catch (error) {
            if (
              isTotalTimeout(request.body.stream, deadlineAt, timeout.signal)
            ) {
              terminalDecided = true;

              return finishTotalTimeout({
                acceptedStream,
                outcome: acceptedStream,
                attempts,
                requestStartedAt,
                upstreamDurationMs,
                request: {
                  log: request.log,
                  reqId: request.reqId,
                },
                reply,
                signal: timeout.signal,
                upstreamLog,
                breaker: opts.breaker,
              });
            }

            try {
              readError = resolveRejection(error, timeout.signal, upstreamLog);
            } catch (unrecognized) {
              terminalDecided = true;

              try {
                opts.breaker.recordResult("INCONCLUSIVE");
              } finally {
                await cancelStream(
                  acceptedStream.reader,
                  upstreamLog,
                  "proxy_route",
                );
              }

              const terminalDurationMs = Date.now() - requestStartedAt;
              const terminalGatewayOverheadMs = Math.max(
                0,
                terminalDurationMs - upstreamDurationMs,
              );

              emitReqComplete(request.log, {
                req_id: request.reqId,
                status: 500,
                error_class: "gateway-fault",
                stream: true,
                attempts,
                duration_ms: terminalDurationMs,
                upstream_duration_ms: upstreamDurationMs,
                gateway_overhead_ms: terminalGatewayOverheadMs,
                retry_disposition: deriveRetryDisposition(
                  attempts,
                  acceptedStream,
                  timeout.signal,
                ),
                terminal: "UNRECOGNIZED_REJECTION",
              });

              throw unrecognized;
            }
          }

          if (isTotalTimeout(request.body.stream, deadlineAt, timeout.signal)) {
            terminalDecided = true;

            return finishTotalTimeout({
              acceptedStream,
              outcome: acceptedStream,
              attempts,
              requestStartedAt,
              upstreamDurationMs,
              request: {
                log: request.log,
                reqId: request.reqId,
              },
              reply,
              signal: timeout.signal,
              upstreamLog,
              breaker: opts.breaker,
            });
          }

          if (readError !== undefined) {
            outcome = readError;
          } else if (firstFrame !== undefined) {
            let terminal: ReqCompleteTerminal | undefined;
            if (
              firstFrame.kind === "eof" ||
              firstFrame.kind === "eof_partial"
            ) {
              terminal = "UPSTREAM_EOF_BEFORE_FIRST_FRAME";
            } else if (firstFrame.kind === "cap") {
              terminal = "PARSER_BUFFER_CAP";
            } else if (
              firstFrame.kind === "frame" &&
              firstFrame.data !== null &&
              firstFrame.data !== "[DONE]"
            ) {
              try {
                JSON.parse(firstFrame.data);
              } catch {
                terminal = "FIRST_FRAME_NOT_JSON";
              }
            }

            if (terminal !== undefined) {
              terminalDecided = true;

              return finishFirstFrameFault(
                {
                  acceptedStream,
                  outcome,
                  attempts,
                  requestStartedAt,
                  upstreamDurationMs,
                  request: {
                    log: request.log,
                    reqId: request.reqId,
                  },
                  reply,
                  signal: timeout.signal,
                  upstreamLog,
                  breaker: opts.breaker,
                },
                terminal,
              );
            }

            if (firstFrame.kind === "frame" && firstFrame.data === "[DONE]") {
              terminalDecided = true;
              reply.hijack();
              reply.raw.writeHead(200, {
                "content-type": "text/event-stream",
                "cache-control": "no-cache",
                connection: "keep-alive",
              });
              reply.raw.write(firstFrame.bytes);
              reply.raw.end();

              try {
                opts.breaker.recordResult("SUCCESS");
              } finally {
                await cancelStream(
                  acceptedStream.reader,
                  upstreamLog,
                  "proxy_route",
                );
              }

              const terminalDurationMs = Date.now() - requestStartedAt;
              const terminalGatewayOverheadMs = Math.max(
                0,
                terminalDurationMs - upstreamDurationMs,
              );

              emitStreamDone(request.log, {
                req_id: request.reqId,
                completed: true,
                terminal: "DONE",
                total_duration_ms: terminalDurationMs,
                upstream_duration_ms: upstreamDurationMs,
                gateway_overhead_ms: terminalGatewayOverheadMs,
                attempts,
                error_class: null,
              });

              return;
            }

            terminalDecided = true;

            const status = 500;
            const errorClass: ErrorClass = "gateway-fault";

            try {
              opts.breaker.recordResult("INCONCLUSIVE");
            } finally {
              await cancelStream(
                acceptedStream.reader,
                upstreamLog,
                "proxy_route",
              );
            }

            const terminalDurationMs = Date.now() - requestStartedAt;
            const terminalGatewayOverheadMs = Math.max(
              0,
              terminalDurationMs - upstreamDurationMs,
            );

            emitReqComplete(request.log, {
              req_id: request.reqId,
              status,
              error_class: errorClass,
              stream: true,
              attempts,
              duration_ms: terminalDurationMs,
              upstream_duration_ms: upstreamDurationMs,
              gateway_overhead_ms: terminalGatewayOverheadMs,
              retry_disposition: deriveRetryDisposition(
                attempts,
                outcome,
                timeout.signal,
              ),
              terminal: "STREAM_NOT_DELIVERED",
            });

            reply.code(status).header("x-gateway-error-class", errorClass);
            return {
              error: {
                message: "streaming response was not delivered",
                type: "internal_error",
                code: "stream_not_delivered",
              },
            };
          }
        }

        if (!isAcceptedStream(outcome)) {
          terminalDecided = true;

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

          const alert = alertFor(terminal.error_class);
          if (alert !== null) {
            emitOperationalAlert(request.log, alert, { req_id: request.reqId });
          }

          const reqCompleteTerminal: ReqCompleteTerminal | undefined =
            outcome.kind === "network_failed" && !outcome.pre_send_proven
              ? "NETWORK_FAILED_POST_SEND"
              : undefined;

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
            terminal: reqCompleteTerminal,
          });

          reply
            .code(terminal.status)
            .header("x-gateway-error-class", terminal.error_class);

          // Sanitized terminals never pass through: the upstream condition is the
          // deployment operator's own (credentials, access, quota), so the
          // upstream's body and headers (Retry-After included) are replaced by
          // the deterministic terminal below.
          if (
            outcome.kind === "upstream_error" &&
            !isSanitizedUpstreamTerminal(terminal.error_class)
          ) {
            if (outcome.retry_after !== undefined) {
              reply.header("retry-after", outcome.retry_after);
            }
            return outcome.body_raw;
          }

          return bodyForErrorOutcome(outcome, terminal.error_class);
        }
      } finally {
        timeout.clear();
      }
    },
  );
};
