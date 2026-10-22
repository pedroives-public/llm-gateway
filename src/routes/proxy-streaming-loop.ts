import type { ServerResponse } from "node:http";
import type { ErrorClass } from "../observability/events.js";
import {
  classify,
  type MinLogger as ClassifierLog,
} from "../upstream/classify.js";
import { resolveRejection, type Logger } from "../upstream/rejection.js";
import type { SseFrame } from "../upstream/sse-frame-reader.js";
import {
  runPostLoop,
  type PostLoopContext,
} from "./proxy-streaming-post-loop.js";
import { isTotalTimeout } from "./proxy-first-frame.js";

type SentinelTerminal =
  | "TOTAL_TIMEOUT"
  | "UPSTREAM_DEATH"
  | "EOF_WITHOUT_DONE"
  | "PARSER_BUFFER_CAP";

type SentinelVocabularyRow = {
  type: "gateway_timeout" | "upstream_error";
  code: string;
  message: string;
};

const SENTINEL_VOCABULARY: Record<SentinelTerminal, SentinelVocabularyRow> = {
  TOTAL_TIMEOUT: {
    type: "gateway_timeout",
    code: "total_timeout_exceeded",
    message: "stream exceeded the total duration limit",
  },
  UPSTREAM_DEATH: {
    type: "upstream_error",
    code: "stream_aborted",
    message: "stream interrupted",
  },
  EOF_WITHOUT_DONE: {
    type: "upstream_error",
    code: "stream_truncated",
    message: "stream ended without completion",
  },
  PARSER_BUFFER_CAP: {
    type: "upstream_error",
    code: "upstream_decode_error",
    message: "stream frame could not be decoded",
  },
};

function streamSentinel(terminal: SentinelTerminal): Uint8Array {
  const sentinel = SENTINEL_VOCABULARY[terminal];
  const body = `{"error": {"message": ${JSON.stringify(sentinel.message)}, "type": ${JSON.stringify(sentinel.type)}, "code": ${JSON.stringify(sentinel.code)}}, "gateway": {"terminal": ${JSON.stringify(terminal)}}}`;

  return new TextEncoder().encode(`data: ${body}\n\n`);
}

export interface ForwardingLoopOptions {
  nextFrame: () => Promise<SseFrame>;
  raw: ServerResponse;
  signal: AbortSignal;
  deadlineAt: number;
  log: ClassifierLog;
  reqId: string;
  upstreamLog: Logger;
  decideTerminal: () => void;
  postLoop: PostLoopContext;
}

// After the first frame: forward every frame's original bytes until the
// reader's result, or a rejection of the read, decides a terminal.
export async function forwardUntilTerminal(
  options: ForwardingLoopOptions,
): Promise<void> {
  const {
    nextFrame,
    raw,
    signal,
    log,
    reqId,
    upstreamLog,
    decideTerminal,
    postLoop,
    deadlineAt,
  } = options;

  const endWithSentinel = (
    terminal: SentinelTerminal,
    errorClass: ErrorClass,
  ): void => {
    decideTerminal();

    raw.write(streamSentinel(terminal));
    raw.end();

    runPostLoop(postLoop, {
      terminal,
      breakerOutcome: "INCONCLUSIVE",
      completed: false,
      errorClass,
    });
  };

  while (true) {
    let frame: SseFrame;

    try {
      frame = await nextFrame();
    } catch (error) {
      if (isTotalTimeout(true, deadlineAt, signal)) {
        endWithSentinel("TOTAL_TIMEOUT", "gateway-fault");
        return;
      }

      const readError = resolveRejection(error, signal, upstreamLog);
      const classification = classify(readError, log, reqId);
      endWithSentinel("UPSTREAM_DEATH", classification.error_class);
      return;
    }

    if (isTotalTimeout(true, deadlineAt, signal)) {
      endWithSentinel("TOTAL_TIMEOUT", "gateway-fault");
      return;
    }

    if (frame.kind !== "frame") {
      const terminal =
        frame.kind === "cap" ? "PARSER_BUFFER_CAP" : "EOF_WITHOUT_DONE";

      endWithSentinel(terminal, "upstream-fault");
      return;
    }

    if (frame.data === "[DONE]") {
      decideTerminal();

      raw.write(frame.bytes);
      raw.end();

      runPostLoop(postLoop, {
        terminal: "DONE",
        breakerOutcome: "SUCCESS",
        completed: true,
        errorClass: null,
      });

      return;
    }

    raw.write(frame.bytes);
  }
}
