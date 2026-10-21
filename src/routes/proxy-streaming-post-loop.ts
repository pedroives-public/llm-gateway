import type { MinLogger } from "../observability/events.js";
import { emitStreamDone } from "../observability/events.js";
import type { ReleaseSlot } from "../reliability/admission-gate.js";
import { cancelStream } from "../upstream/stream.js";

export interface FinishDoneOptions {
  log: MinLogger;
  reqId: string;
  requestStartedAt: number;
  upstreamDurationMs: number;
  attempts: number;
  clearDeadline: () => void;
  recordSuccess: () => void;
  releaseSlot: ReleaseSlot | null;
  reader: ReadableStreamDefaultReader<Uint8Array>;
  upstreamLog: MinLogger;
}

export function finishDone(options: FinishDoneOptions): void {
  const {
    log,
    reqId,
    requestStartedAt,
    upstreamDurationMs,
    attempts,
    clearDeadline,
    recordSuccess,
    releaseSlot,
    reader,
    upstreamLog,
  } = options;

  clearDeadline();
  cancelStream(reader, upstreamLog, "proxy_route");
  releaseSlot?.();
  recordSuccess();

  const totalDurationMs = Date.now() - requestStartedAt;
  const gatewayOverheadMs = Math.max(0, totalDurationMs - upstreamDurationMs);

  emitStreamDone(log, {
    req_id: reqId,
    completed: true,
    terminal: "DONE",
    total_duration_ms: totalDurationMs,
    upstream_duration_ms: upstreamDurationMs,
    gateway_overhead_ms: gatewayOverheadMs,
    attempts,
    error_class: null,
  });
}
