import http from "node:http";
import { describe, it, expect, vi } from "vitest";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { AcceptedStream } from "../../src/upstream/stream.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { bufferedTripwire } from "../helpers/streaming-seams.js";
import { makeLogCapture } from "../log-capture.js";

// The classifier is pure and cannot be made to throw from outside, so one row
// forces it: the mock passes every call through to the real classifier until
// a row arms a single throw. The mock is file-wide, so these rows live in
// their own file.
const classifierFault = vi.hoisted(() => {
  const state: { armed: boolean } = { armed: false };
  return state;
});

vi.mock("../../src/upstream/classify.js", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../src/upstream/classify.js")>();

  return {
    ...actual,
    classify: (...args: Parameters<typeof actual.classify>) => {
      if (classifierFault.armed) {
        classifierFault.armed = false;
        throw new TypeError("a defect inside the classifier");
      }
      return actual.classify(...args);
    },
  };
});

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};
const ONE_CONTENT_FRAME =
  'data: {"id": "c1", "choices": [{"delta": {"content": "Hel"}}]}\n\n';
const SECOND_FRAME =
  'data: {"id": "c1", "choices": [{"delta": {"content": "lo"}}]}\n\n';
const INTERNAL_EXCEPTION_SENTINEL =
  'data: {"error": {"message": "stream ended by an internal error", "type": "internal_error", "code": "unhandled_exception"}, "gateway": {"terminal": "INTERNAL_EXCEPTION"}}\n\n';
const RESET_AFTER_FIRST_FRAME = Object.assign(new TypeError("terminated"), {
  cause: Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }),
});
// Pino's base keys, the request id the route binds, and the one allowlisted
// field a failed terminal write carries.
const ALLOWED_LINE_KEYS = new Set([
  "level",
  "time",
  "pid",
  "hostname",
  "reqId",
  "req_id",
  "msg",
  "err_name",
]);
const RACE_TURNS = 200;
// Turns left for a second terminal to show up after the first one, so its
// absence is read after the loop had every chance to produce it.
const AFTERMATH_TURNS = 20;
const NOT_UNRECOGNIZED_RULE =
  "a gateway exception is not an unrecognized upstream rejection";
const ONE_TERMINAL_RULE =
  "a gateway exception still ends in one terminal and the post-loop";
const TERMINAL_WRITE_RULE =
  "a failed terminal write neither escapes nor creates a second terminal";

function recordingBreaker(): {
  breaker: CircuitBreaker;
  recorded: ProbeOutcome[];
} {
  const recorded: ProbeOutcome[] = [];
  return {
    recorded,
    breaker: {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (outcome) => {
        recorded.push(outcome);
      },
      getState: () => "CLOSED",
    },
  };
}

// An accepted stream whose frames the test releases by hand and whose read it
// can fail with a transport error.
function drivenAcceptedStream(signal: AbortSignal): {
  stream: AcceptedStream;
  release: (text: string) => void;
  fail: (error: unknown) => void;
} {
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  const started = () => {
    if (controller === undefined) {
      throw new Error("the fake stream has not started");
    }
    return controller;
  };

  const body = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
      signal.addEventListener(
        "abort",
        () => {
          c.error(signal.reason);
        },
        { once: true },
      );
    },
    pull() {
      return new Promise<void>(() => {});
    },
  });

  return {
    stream: { kind: "accepted_stream", status: 200, reader: body.getReader() },
    release: (text) => {
      started().enqueue(new TextEncoder().encode(text));
    },
    fail: (error) => {
      started().error(error);
    },
  };
}

const yieldToIo = () =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
const until = async (ready: () => boolean, sentence: string) => {
  for (let turn = 0; turn < RACE_TURNS; turn += 1) {
    if (ready()) return;
    await yieldToIo();
  }
  throw new Error(sentence);
};

// A real socket, so the test holds the very response object the loop writes
// to and can make its write() throw after the first frame crossed the line.
async function committedStream() {
  const capture = makeLogCapture();
  const violations: string[] = [];
  const { breaker, recorded } = recordingBreaker();
  let driven: ReturnType<typeof drivenAcceptedStream> | undefined;

  const app = await buildProxyApp({
    breaker,
    logger: capture.logger,
    streamingEnabled: true,
    upstreamBuffered: bufferedTripwire(violations).seam,
    upstreamStreaming: (_body, signal) => {
      driven = drivenAcceptedStream(signal);
      return Promise.resolve(driven.stream);
    },
  });
  let serverResponse: http.ServerResponse | undefined;
  app.server.on("request", (_req, res: http.ServerResponse) => {
    serverResponse = res;
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  const address = app.server.address();
  if (address === null || typeof address === "string") {
    throw new Error("the app did not listen on a TCP port");
  }

  const body = JSON.stringify({ ...validBody, stream: true });
  const client = http.request({
    host: "127.0.0.1",
    port: address.port,
    method: "POST",
    path: "/v1/chat/completions",
    headers: {
      authorization: bearer(),
      "content-type": "application/json",
      "content-length": String(Buffer.byteLength(body)),
    },
  });
  client.on("error", () => {});
  const received: Buffer[] = [];
  let clientEnded = false;
  client.on("response", (res) => {
    res.on("data", (chunk: Buffer) => {
      received.push(chunk);
    });
    res.on("end", () => {
      clientEnded = true;
    });
  });
  const close = async () => {
    client.destroy();
    classifierFault.armed = false;
    await app.close();
  };

  try {
    client.end(body);
    await until(
      () => driven !== undefined,
      "the scene's premise: the streaming upstream was called once and answered with an accepted head",
    );
    const upstream = driven;
    if (upstream === undefined) {
      throw new Error("the streaming upstream was not called");
    }
    upstream.release(ONE_CONTENT_FRAME);
    await until(
      () =>
        Buffer.concat(received).length >=
        Buffer.byteLength(ONE_CONTENT_FRAME),
      "the scene's premise: the client received the first frame, so the stream crossed the first-frame line",
    );
    expect(violations).toStrictEqual([]);
    const response = serverResponse;
    if (response === undefined) {
      throw new Error("the server response was not captured");
    }

    return {
      capture,
      recorded,
      response,
      upstream,
      received: () => Buffer.concat(received).toString("utf8"),
      clientEnded: () => clientEnded,
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}

type Scene = Awaited<ReturnType<typeof committedStream>>;

describe("streaming flag ON: after the first-frame line, a gateway exception is INTERNAL_EXCEPTION", () => {
  it.each([
    {
      row: "the first write inside the loop throwing once",
      act: ({ response, upstream }: Scene) => {
        vi.spyOn(response, "write").mockImplementationOnce(() => {
          throw new Error("the socket write failed");
        });
        upstream.release(SECOND_FRAME);
      },
      body: ONE_CONTENT_FRAME + INTERNAL_EXCEPTION_SENTINEL,
      endsResponse: true,
      lines: [],
      absence: ONE_TERMINAL_RULE,
      rule: NOT_UNRECOGNIZED_RULE,
    },
    {
      row: "every write inside the loop throwing",
      act: ({ response, upstream }: Scene) => {
        vi.spyOn(response, "write").mockImplementation(() => {
          throw new Error("the socket write failed");
        });
        upstream.release(SECOND_FRAME);
      },
      body: ONE_CONTENT_FRAME,
      endsResponse: false,
      // The absorbed sentinel write still leaves a trace: one line naming the
      // failure, so a terminal the client never received is not silent.
      lines: [{ msg: "STREAM_TERMINAL_WRITE_FAILED", err_name: "Error" }],
      absence: TERMINAL_WRITE_RULE,
      rule: TERMINAL_WRITE_RULE,
    },
    {
      row: "the classifier throwing on a recognized network failure",
      act: ({ upstream }: Scene) => {
        classifierFault.armed = true;
        upstream.fail(RESET_AFTER_FIRST_FRAME);
      },
      body: ONE_CONTENT_FRAME + INTERNAL_EXCEPTION_SENTINEL,
      endsResponse: true,
      lines: [],
      absence: ONE_TERMINAL_RULE,
      rule: NOT_UNRECOGNIZED_RULE,
    },
  ])("ends as INTERNAL_EXCEPTION with $row", async ({ act, body, endsResponse, lines, absence, rule }) => {
    const s = await committedStream();

    try {
      act(s);
      await until(() => s.capture.byEvent("stream_done").length > 0, absence);
      if (endsResponse) {
        await until(
          s.clientEnded,
          `${rule}: the sentinel is written and the response is ended`,
        );
      }
      for (let turn = 0; turn < AFTERMATH_TURNS; turn += 1) {
        await yieldToIo();
      }

      expect(
        s.capture.byEvent("stream_done"),
        `${absence}: exactly one terminal event`,
      ).toHaveLength(1);
      expect(s.capture.byEvent("stream_done")[0], rule).toMatchObject({
        completed: false,
        terminal: "INTERNAL_EXCEPTION",
        error_class: "gateway-fault",
      });
      expect(
        s.received(),
        `${rule}: the client sees the forwarded first frame and, when a write can still succeed, exactly one INTERNAL_EXCEPTION sentinel`,
      ).toBe(body);
      expect(
        s.recorded,
        "a gateway exception votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
      const warnings = s.capture.logs.filter(
        (log) => typeof log.level === "number" && log.level >= 40,
      );
      expect(
        warnings.map((log) => ({ msg: log.msg, err_name: log.err_name })),
        `${absence}: nothing is thrown to the framework's error handler, and an absorbed terminal write leaves one line`,
      ).toStrictEqual(lines);
      for (const line of warnings) {
        expect(
          new Set(Object.keys(line)),
          "a diagnostic line carries only allowlisted fields: never an error message, a body or a credential",
        ).toEqual(ALLOWED_LINE_KEYS);
      }
    } finally {
      await s.close();
    }
  });
});
