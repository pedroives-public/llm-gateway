import type { ServerResponse } from "node:http";
import { once } from "node:events";
import type { ErrorClass } from "../observability/events.js";
import {
  classify,
  type MinLogger as ClassifierLog,
} from "../upstream/classify.js";
import {
  resolveRejection,
  extractErrName,
  type Logger,
} from "../upstream/rejection.js";
import type { SseFrame } from "../upstream/sse-frame-reader.js";
import {
  runPostLoop,
  type PostLoopContext,
} from "./proxy-streaming-post-loop.js";
import { deadlineConditionHolds } from "./proxy-first-frame.js";
import { extractCauseLog } from "../upstream/cause-log.js";
import type { TotalDurationTimeout } from "../reliability/timeouts.js";

type SentinelTerminal =
  | "TOTAL_TIMEOUT"
  | "UPSTREAM_DEATH"
  | "EOF_WITHOUT_DONE"
  | "PARSER_BUFFER_CAP"
  | "UNRECOGNIZED_REJECTION"
  | "INTERNAL_EXCEPTION";

type SentinelVocabularyRow = {
  type: "gateway_timeout" | "upstream_error" | "internal_error";
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
  UNRECOGNIZED_REJECTION: {
    type: "internal_error",
    code: "unhandled_exception",
    message: "stream ended by an internal error",
  },
  INTERNAL_EXCEPTION: {
    type: "internal_error",
    code: "unhandled_exception",
    message: "stream ended by an internal error",
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
  abortForTerminalAfterDecision: TotalDurationTimeout["abortForTerminalAfterDecision"];
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
    abortForTerminalAfterDecision,
  } = options;

  // Whether this loop already decided the stream's terminal. Only a boundary
  // catch reads it: an exception that arrives after the decision was thrown
  // by the terminal block itself, which must not run a second time.
  let terminalDecided = false;
  const decide = (): void => {
    terminalDecided = true;
    decideTerminal();
  };

  const endWithSentinel = (
    terminal: SentinelTerminal,
    errorClass: ErrorClass,
  ): void => {
    // Built before the decision and outside the try: a defect in building the
    // sentinel is the gateway's own exception, and it must reach the boundary
    // catch while no terminal is decided, so that catch can still end the
    // stream.
    const sentinel = streamSentinel(terminal);

    decide();

    try {
      raw.write(sentinel);
      raw.end();
    } catch (error) {
      // The terminal decision and post-loop still must run when the response
      // cannot accept the sentinel, and the client never receiving it must
      // leave a trace.
      upstreamLog.error(
        { err_name: extractErrName(error) },
        "STREAM_TERMINAL_WRITE_FAILED",
      );
    }

    if (terminal === "TOTAL_TIMEOUT") {
      abortForTerminalAfterDecision("total_timeout");
    } else if (terminal === "PARSER_BUFFER_CAP") {
      abortForTerminalAfterDecision("parser_buffer_cap");
    }

    runPostLoop(postLoop, {
      terminal,
      breakerOutcome: "INCONCLUSIVE",
      completed: false,
      errorClass,
    });
  };

  // The client left: write nothing, not even end(), because no receiver
  // exists. The level form is read because a destroy is visible at once,
  // before the departure observer's close event arrives.
  const finishClientGone = (): void => {
    decide();
    abortForTerminalAfterDecision("client_gone");

    runPostLoop(postLoop, {
      terminal: "CLIENT_GONE",
      breakerOutcome: "INCONCLUSIVE",
      completed: false,
      errorClass: "client-fault",
    });
  };

  // A boundary catch holds the gateway's own exception. Before the decision it
  // ends the stream as INTERNAL_EXCEPTION. After it, the throw came from the
  // terminal block, which already ran up to that throw; running it again
  // would write after end() and vote a second time, so the line naming the
  // throw is the only effect left.
  const absorbAtBoundary = (error: unknown): void => {
    const errName = extractErrName(error);

    if (terminalDecided) {
      upstreamLog.error({ err_name: errName }, "STREAM_THROW_AFTER_TERMINAL");
      return;
    }

    upstreamLog.error({ err_name: errName }, "STREAM_INTERNAL_EXCEPTION");
    endWithSentinel("INTERNAL_EXCEPTION", "gateway-fault");
  };

  while (true) {
    let frame: SseFrame;

    try {
      frame = await nextFrame();
    } catch (error) {
      if (raw.destroyed) {
        finishClientGone();
        return;
      }

      if (deadlineConditionHolds(deadlineAt, signal)) {
        endWithSentinel("TOTAL_TIMEOUT", "gateway-fault");
        return;
      }

      try {
        let readError: ReturnType<typeof resolveRejection>;

        try {
          readError = resolveRejection(error, signal, upstreamLog);
        } catch {
          endWithSentinel("UNRECOGNIZED_REJECTION", "gateway-fault");
          return;
        }

        if (readError.kind !== "network_failed") {
          upstreamLog.error(
            {
              ...extractCauseLog(error),
              err_name: extractErrName(error),
              abort_identity: error === signal.reason,
            },
            "STREAM_REJECTION_OUTCOME_UNHANDLED",
          );
          endWithSentinel("UNRECOGNIZED_REJECTION", "gateway-fault");
          return;
        }

        const classification = classify(readError, log, reqId);
        endWithSentinel("UPSTREAM_DEATH", classification.error_class);
        return;
      } catch (error) {
        absorbAtBoundary(error);
        return;
      }
    }

    if (raw.destroyed) {
      finishClientGone();
      return;
    }

    if (deadlineConditionHolds(deadlineAt, signal)) {
      endWithSentinel("TOTAL_TIMEOUT", "gateway-fault");
      return;
    }

    try {
      if (frame.kind !== "frame") {
        const terminal =
          frame.kind === "cap" ? "PARSER_BUFFER_CAP" : "EOF_WITHOUT_DONE";

        endWithSentinel(terminal, "upstream-fault");
        return;
      }

      if (frame.data === "[DONE]") {
        decide();

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

      const needsDrain = !raw.write(frame.bytes);

      if (needsDrain) {
        try {
          await once(raw, "drain", { signal });
        } catch {
          if (raw.destroyed) {
            finishClientGone();
            return;
          }

          if (deadlineConditionHolds(deadlineAt, signal)) {
            endWithSentinel("TOTAL_TIMEOUT", "gateway-fault");
            return;
          }

          // Unreachable by design: an abort of the drain wait comes from a
          // client departure or the deadline, which the checks above read.
          // Ending here keeps the loop from leaving without a terminal.
          endWithSentinel("UNRECOGNIZED_REJECTION", "gateway-fault");
          return;
        }
      }
    } catch (error) {
      absorbAtBoundary(error);
      return;
    }
  }
}
