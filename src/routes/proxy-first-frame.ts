import type { FastifyReply } from "fastify";
import type { CircuitBreaker } from "../reliability/circuit-breaker.js";
import type { Logger } from "../upstream/rejection.js";
import {
  emitReqComplete,
  type ErrorClass,
  type MinLogger,
  type ReqCompleteTerminal,
} from "../observability/events.js";
import {
  cancelStream,
  type AcceptedStream,
  type AttemptResult,
} from "../upstream/stream.js";
import {
  deriveRetryDisposition,
  TOTAL_TIMEOUT_BODY,
  UPSTREAM_DECODE_ERROR_BODY,
} from "./proxy-error-outcome.js";

// Terminals the streaming branch reaches before a first frame is committed to
// the client: the total-duration deadline (504, one INCONCLUSIVE vote) and the
// first-frame faults (502, one FAILURE vote). Each takes its context
// explicitly, so the route handler keeps only the decision of WHICH terminal
// applies. The flag that tells the deadline timer a terminal is already
// settled stays in the handler: it is closure state of the request, set
// before any of these run.

// The deadline condition holds when the re-read clock has reached the
// deadline, or when its own timer has already aborted: that timer aborts only
// after reading the deadline value, so the reason on the signal records a
// reading that held, even if the clock has stepped back since. The reason is
// read, not the aborted flag, because the flag cannot tell the deadline's
// abort from any other cause that comes to share this signal.
export function deadlineConditionHolds(
  deadlineAt: number,
  signal: AbortSignal,
): boolean {
  return Date.now() >= deadlineAt || signal.reason?.kind === "total_timeout";
}

// Before the fork both transports share the request's deadline value; only on
// a streaming request is that value the total-duration deadline.
export function isTotalTimeout(
  stream: boolean | undefined,
  deadlineAt: number,
  signal: AbortSignal,
): boolean {
  return stream === true && deadlineConditionHolds(deadlineAt, signal);
}

type TotalTimeoutContext = {
  acceptedStream?: AcceptedStream;
  outcome: AttemptResult;
  attempts: number;
  requestStartedAt: number;
  upstreamDurationMs: number;
  request: {
    log: MinLogger;
    reqId: string;
  };
  reply: FastifyReply;
  signal: AbortSignal;
  upstreamLog: Logger & MinLogger;
  breaker: CircuitBreaker;
};

export async function finishTotalTimeout(
  context: TotalTimeoutContext,
): Promise<typeof TOTAL_TIMEOUT_BODY> {
  const status = 504;
  const errorClass: ErrorClass = "gateway-fault";

  try {
    context.breaker.recordResult("INCONCLUSIVE");
  } finally {
    if (context.acceptedStream !== undefined) {
      await cancelStream(
        context.acceptedStream.reader,
        context.upstreamLog,
        "proxy_route",
      );
    }
  }

  const terminalDurationMs = Date.now() - context.requestStartedAt;
  const terminalGatewayOverheadMs = Math.max(
    0,
    terminalDurationMs - context.upstreamDurationMs,
  );

  emitReqComplete(context.request.log, {
    req_id: context.request.reqId,
    status,
    error_class: errorClass,
    stream: true,
    attempts: context.attempts,
    duration_ms: terminalDurationMs,
    upstream_duration_ms: context.upstreamDurationMs,
    gateway_overhead_ms: terminalGatewayOverheadMs,
    retry_disposition: deriveRetryDisposition(
      context.attempts,
      context.outcome,
      context.signal,
    ),
    terminal: "TOTAL_TIMEOUT",
  });

  context.reply.code(status).header("x-gateway-error-class", errorClass);
  return TOTAL_TIMEOUT_BODY;
}

// The 502 the streaming branch answers when the first frame cannot be
// committed: the body ended before a frame, the parser buffer cap was hit, or
// the frame's data is not JSON. Which of the three applies is the caller's
// decision (`terminal`); the vote, the upstream teardown, the request-complete
// event and the sanitized body are the same for all three.
export type FirstFrameFaultContext = Omit<
  TotalTimeoutContext,
  "acceptedStream"
> & {
  acceptedStream: AcceptedStream;
};

export async function finishFirstFrameFault(
  context: FirstFrameFaultContext,
  terminal: ReqCompleteTerminal,
): Promise<typeof UPSTREAM_DECODE_ERROR_BODY> {
  const status = 502;
  const errorClass: ErrorClass = "upstream-fault";

  try {
    context.breaker.recordResult("FAILURE");
  } finally {
    await cancelStream(
      context.acceptedStream.reader,
      context.upstreamLog,
      "proxy_route",
    );
  }

  const terminalDurationMs = Date.now() - context.requestStartedAt;
  const terminalGatewayOverheadMs = Math.max(
    0,
    terminalDurationMs - context.upstreamDurationMs,
  );

  emitReqComplete(context.request.log, {
    req_id: context.request.reqId,
    status,
    error_class: errorClass,
    stream: true,
    attempts: context.attempts,
    duration_ms: terminalDurationMs,
    upstream_duration_ms: context.upstreamDurationMs,
    gateway_overhead_ms: terminalGatewayOverheadMs,
    retry_disposition: deriveRetryDisposition(
      context.attempts,
      context.outcome,
      context.signal,
    ),
    terminal,
  });

  context.reply.code(status).header("x-gateway-error-class", errorClass);
  return UPSTREAM_DECODE_ERROR_BODY;
}
