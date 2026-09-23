import type {
  Outcome,
  ErrorOutcome,
} from "./outcome.js";
import { assertNever } from "./assert-never.js";
import type { MinLogger } from "../observability/events.js";
import { emitUpstreamCancelRejected } from "../observability/events.js";
import { extractErrName } from "./rejection.js";

export type AcceptedStream = {
  kind: "accepted_stream";
  status: number;
  reader: ReadableStreamDefaultReader<Uint8Array>;
};

export type AttemptResult = Outcome | AcceptedStream;
export type StreamingAdapter = AcceptedStream | ErrorOutcome;

export function isAcceptedStream(
  result: AttemptResult,
): result is AcceptedStream {
  switch (result.kind) {
    case "accepted_stream":
      return true;
    case "upstream_error":
    case "undecodable":
    case "network_failed":
    case "redirect_blocked":
    case "ok":
    case "aborted":
      return false;
    default:
      return assertNever(result);
  }
}

export async function cancelStream(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  log: MinLogger,
  site: "upstream_adapter" | "proxy_route",
): Promise<void> {
  try {
    await reader.cancel();
  } catch (error) {
    // A reader whose underlying body already errored rejects cancel(). The
    // body is already closed in that case, so this is a cleanup observation,
    // not a new request terminal.
    emitUpstreamCancelRejected(log, {
      site,
      err_name: extractErrName(error),
    });
  }
}
