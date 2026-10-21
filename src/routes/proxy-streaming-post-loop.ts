import type {
  ErrorClass,
  MinLogger,
  StreamDoneTerminal,
} from "../observability/events.js";
import { emitStreamDone } from "../observability/events.js";
import type { ReleaseSlot } from "../reliability/admission-gate.js";
import { cancelStream } from "../upstream/stream.js";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../reliability/circuit-breaker.js";

// What every committed stream's ending shares, built once after the attempt.
export interface PostLoopContext {
  log: MinLogger;
  reqId: string;
  requestStartedAt: number;
  upstreamDurationMs: number;
  attempts: number;
  clearDeadline: () => void;
  breaker: CircuitBreaker;
  releaseSlot: ReleaseSlot | null;
  reader: ReadableStreamDefaultReader<Uint8Array>;
  upstreamLog: MinLogger;
}

// What one terminal decides.
export interface StreamTerminalOutcome {
  terminal: StreamDoneTerminal;
  breakerOutcome: ProbeOutcome;
  completed: boolean;
  errorClass: ErrorClass | null;
}

export function runPostLoop(
  context: PostLoopContext,
  outcome: StreamTerminalOutcome,
): void {
  const {
    log,
    reqId,
    requestStartedAt,
    upstreamDurationMs,
    attempts,
    clearDeadline,
    breaker,
    releaseSlot,
    reader,
    upstreamLog,
  } = context;
  const { terminal, breakerOutcome, completed, errorClass } = outcome;

  clearDeadline();
  void cancelStream(reader, upstreamLog, "proxy_route");
  releaseSlot?.();
  breaker.recordResult(breakerOutcome);

  const totalDurationMs = Date.now() - requestStartedAt;
  const gatewayOverheadMs = Math.max(0, totalDurationMs - upstreamDurationMs);

  emitStreamDone(log, {
    req_id: reqId,
    completed,
    terminal,
    total_duration_ms: totalDurationMs,
    upstream_duration_ms: upstreamDurationMs,
    gateway_overhead_ms: gatewayOverheadMs,
    attempts,
    error_class: errorClass,
  });
}
