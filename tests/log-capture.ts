import { Writable } from "node:stream";
import { StringDecoder } from "node:string_decoder";

// Capture the gateway's structured Pino events as they flow through the real
// request.log path inside buildApp. Unit tests (tests/observability/events.test.ts)
// pass a fake { info } logger to the emitters directly; integration tests log
// through Fastify's request.log, so the only way to observe events is to capture
// Pino's output stream (which also exercises serialization).

// One parsed Pino NDJSON log line. Every emitter in src/observability/events.ts
// writes `{ event, ...payload }`, so `event` discriminates the gateway's own
// structured events from Fastify's built-in request logs (which carry no `event`).
export interface CapturedLog {
  event?: string;
  [key: string]: unknown;
}

export interface LogCapture {
  // The value handed to buildApp({ logger }). buildApp forwards it to
  // Fastify({ logger }), so this must be a Pino-compatible logger options object
  // (or `false` to disable). `false` captures nothing.
  logger: boolean | Record<string, unknown>;
  logs: CapturedLog[];
  byEvent: (event: string) => CapturedLog[];
}

export function makeLogCapture(): LogCapture {
  const logs: CapturedLog[] = [];

  const decoder = new StringDecoder("utf8");
  let buffer = "";

  const stream = new Writable({
    write(chunk, _enc, callback) {
      // Pino writes one NDJSON line per log; a chunk may split a line (or a
      // multi-byte char, hence StringDecoder), so reassemble across writes.
      buffer += decoder.write(chunk);

      const parts = buffer.split("\n");
      buffer = parts.pop() ?? "";

      parts.forEach((line) => {
        if (line) {
          const captured: CapturedLog = JSON.parse(line);
          logs.push(captured);
        }
      });

      callback();
    },
  });

  // Must stay a direct stream, never a transport: a worker-thread transport
  // makes capture async/cross-thread and breaks the tests' synchronous reads.
  const logger: LogCapture["logger"] = { stream, level: "trace" };

  return {
    logger,
    logs,
    byEvent: (event) => logs.filter((log) => log.event === event),
  };
}
