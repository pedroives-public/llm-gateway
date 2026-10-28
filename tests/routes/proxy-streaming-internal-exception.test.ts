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
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

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
const DONE_FRAME = "data: [DONE]\n\n";
const INTERNAL_EXCEPTION_SENTINEL =
  'data: {"error": {"message": "stream ended by an internal error", "type": "internal_error", "code": "unhandled_exception"}, "gateway": {"terminal": "INTERNAL_EXCEPTION"}}\n\n';
const UPSTREAM_DEATH_SENTINEL =
  'data: {"error": {"message": "stream interrupted", "type": "upstream_error", "code": "stream_aborted"}, "gateway": {"terminal": "UPSTREAM_DEATH"}}\n\n';
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
const BASE_LINE_KEYS = new Set([
  "level",
  "time",
  "pid",
  "hostname",
  "reqId",
  "req_id",
]);
const lineFields = (line: CapturedLog): Record<string, unknown> =>
  Object.fromEntries(
    Object.entries(line).filter(([key]) => !BASE_LINE_KEYS.has(key)),
  );
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
const ENDED_RULE =
  "every terminal but CLIENT_GONE ends the response, even when its terminal write failed";
const ONE_BLOCK_RULE =
  "a decided terminal runs its terminal block at most once";
const CLIENT_STEP_RULE =
  "a failure in the terminal block's client step leaves one line, and the post-loop still runs once";

// The vote is the one post-loop step a fake can make throw, so a row that
// needs the post-loop to fail after its terminal is decided arms it here.
function recordingBreaker(voteThrowsOnce: boolean): {
  breaker: CircuitBreaker;
  recorded: ProbeOutcome[];
} {
  const recorded: ProbeOutcome[] = [];
  let voteFaultArmed = voteThrowsOnce;
  return {
    recorded,
    breaker: {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (outcome) => {
        recorded.push(outcome);
        if (voteFaultArmed) {
          voteFaultArmed = false;
          throw new Error("a defect inside the breaker vote");
        }
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
async function committedStream(
  options: { voteThrowsOnce: boolean } = { voteThrowsOnce: false },
) {
  const capture = makeLogCapture();
  const violations: string[] = [];
  const { breaker, recorded } = recordingBreaker(options.voteThrowsOnce);
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
      // The client leaves without reading on: its socket closes, so the
      // server response closes and the departure observer fires.
      depart: () => {
        client.destroy();
      },
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
      lines: [{ msg: "STREAM_INTERNAL_EXCEPTION", err_name: "Error" }],
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
      endsResponse: true,
      // Two absorbed failures, two lines in order: the forwarding write's,
      // caught before any terminal is decided, then the sentinel write's, so a
      // terminal the client never received is not silent. The response still
      // ends: a failed sentinel write does not skip end().
      lines: [
        { msg: "STREAM_INTERNAL_EXCEPTION", err_name: "Error" },
        { msg: "STREAM_TERMINAL_WRITE_FAILED", err_name: "Error" },
      ],
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
      lines: [{ msg: "STREAM_INTERNAL_EXCEPTION", err_name: "TypeError" }],
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
          `${ENDED_RULE}: the client saw the response end`,
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
        `${absence}: nothing is thrown to the framework's error handler, and every absorbed exception leaves one line naming it`,
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

describe("streaming flag ON: a decided terminal runs its terminal block at most once", () => {
  it.each([
    {
      row: "UPSTREAM_DEATH",
      act: ({ upstream }: Scene) => {
        upstream.fail(RESET_AFTER_FIRST_FRAME);
      },
      body: ONE_CONTENT_FRAME + UPSTREAM_DEATH_SENTINEL,
      votes: ["INCONCLUSIVE"],
      // The classified failure leaves its cause line before the absorbed
      // throw leaves its own.
      lines: [
        {
          cause_code: RESET_AFTER_FIRST_FRAME.cause.code,
          cause_name: RESET_AFTER_FIRST_FRAME.cause.name,
        },
        { msg: "STREAM_THROW_AFTER_TERMINAL", err_name: "Error" },
      ],
    },
    {
      row: "[DONE]",
      act: ({ upstream }: Scene) => {
        upstream.release(DONE_FRAME);
      },
      body: ONE_CONTENT_FRAME + DONE_FRAME,
      votes: ["SUCCESS"],
      lines: [{ msg: "STREAM_THROW_AFTER_TERMINAL", err_name: "Error" }],
    },
  ])(
    "absorbs a post-loop throw after $row without a second terminal block",
    async ({ act, body, votes, lines }) => {
      const s = await committedStream({ voteThrowsOnce: true });
      const write = vi.spyOn(s.response, "write");
      const end = vi.spyOn(s.response, "end");

      try {
        act(s);
        await until(
          s.clientEnded,
          `${ONE_BLOCK_RULE}: the decided terminal writes its closing bytes and ends the response`,
        );
        for (let turn = 0; turn < AFTERMATH_TURNS; turn += 1) {
          await yieldToIo();
        }
        const responseCalls = [
          ...write.mock.invocationCallOrder.map((at) => ({
            at,
            call: "write",
          })),
          ...end.mock.invocationCallOrder.map((at) => ({ at, call: "end" })),
        ]
          .toSorted((a, b) => a.at - b.at)
          .map(({ call }) => call);
        const warnings = s.capture.logs.filter(
          (log) => typeof log.level === "number" && log.level >= 40,
        );

        expect(
          responseCalls,
          `${ONE_BLOCK_RULE}: after the first frame the response sees one closing write and one end(), and nothing after end()`,
        ).toStrictEqual(["write", "end"]);
        expect(
          s.received(),
          `${ONE_BLOCK_RULE}: the client sees the forwarded first frame and the decided terminal's closing bytes, once`,
        ).toBe(body);
        expect(
          s.recorded,
          `${ONE_BLOCK_RULE}: the breaker sees one vote, the decided terminal's`,
        ).toStrictEqual(votes);
        expect(
          s.capture.byEvent("stream_done"),
          `${ONE_BLOCK_RULE}: no second terminal event (a post-loop that throws before its event loses it, an accepted residual)`,
        ).toStrictEqual([]);
        // Every warn-or-above line without Pino's base keys and the request
        // ids, so the exact list allowlists every line and every field: a
        // framework error line would add its message and stack.
        expect(
          warnings.map(lineFields),
          `${ONE_BLOCK_RULE}: the absorbed throw leaves one diagnostic line, and nothing reaches the framework's error handler`,
        ).toStrictEqual(lines);
      } finally {
        await s.close();
      }
    },
  );

  // A departure decides CLIENT_GONE outside a wake's interpretation, so only
  // a boundary around the whole loop can hold a throw from its post-loop.
  it.each([
    {
      row: "a client departure",
      act: ({ depart }: Scene) => {
        depart();
      },
      body: ONE_CONTENT_FRAME,
      calls: [],
      votes: ["INCONCLUSIVE"],
      events: [],
      lines: [{ msg: "STREAM_THROW_AFTER_TERMINAL", err_name: "Error" }],
    },
  ])(
    "absorbs a post-loop throw after $row without a second terminal block",
    async ({ act, body, calls, votes, events, lines }) => {
      const s = await committedStream({ voteThrowsOnce: true });
      const write = vi.spyOn(s.response, "write");
      const end = vi.spyOn(s.response, "end");

      try {
        act(s);
        await until(
          () => s.recorded.length > 0,
          `${ONE_BLOCK_RULE}: the departure's post-loop reaches the breaker vote`,
        );
        for (let turn = 0; turn < AFTERMATH_TURNS; turn += 1) {
          await yieldToIo();
        }
        const responseCalls = [
          ...write.mock.invocationCallOrder.map((at) => ({
            at,
            call: "write",
          })),
          ...end.mock.invocationCallOrder.map((at) => ({ at, call: "end" })),
        ]
          .toSorted((a, b) => a.at - b.at)
          .map(({ call }) => call);
        const warnings = s.capture.logs.filter(
          (log) => typeof log.level === "number" && log.level >= 40,
        );

        expect(
          responseCalls,
          `${ONE_BLOCK_RULE}: a departed client gets no write and no end()`,
        ).toStrictEqual(calls);
        expect(
          s.received(),
          `${ONE_BLOCK_RULE}: the client keeps only what it read before leaving`,
        ).toBe(body);
        expect(
          s.recorded,
          `${ONE_BLOCK_RULE}: the breaker sees one vote, the departure's`,
        ).toStrictEqual(votes);
        expect(
          s.capture.byEvent("stream_done"),
          `${ONE_BLOCK_RULE}: no second terminal event (a post-loop that throws before its event loses it, an accepted residual)`,
        ).toStrictEqual(events);
        expect(
          warnings.map(lineFields),
          `${ONE_BLOCK_RULE}: the absorbed throw leaves one diagnostic line, and nothing reaches the framework's error handler`,
        ).toStrictEqual(lines);
      } finally {
        await s.close();
      }
    },
  );
});

describe("streaming flag ON: a failure in the terminal block's client step is absorbed", () => {
  it.each([
    {
      row: "end() throwing after the UPSTREAM_DEATH sentinel",
      act: ({ response, upstream }: Scene) => {
        vi.spyOn(response, "end").mockImplementationOnce(() => {
          throw new Error("the response end failed");
        });
        upstream.fail(RESET_AFTER_FIRST_FRAME);
      },
      body: ONE_CONTENT_FRAME + UPSTREAM_DEATH_SENTINEL,
      endsResponse: false,
      terminal: "UPSTREAM_DEATH",
      completed: false,
      votes: ["INCONCLUSIVE"],
      lines: [
        {
          cause_code: RESET_AFTER_FIRST_FRAME.cause.code,
          cause_name: RESET_AFTER_FIRST_FRAME.cause.name,
        },
        { msg: "STREAM_TERMINAL_END_FAILED", err_name: "Error" },
      ],
    },
    {
      row: "the [DONE] frame's write throwing",
      act: ({ response, upstream }: Scene) => {
        vi.spyOn(response, "write").mockImplementationOnce(() => {
          throw new Error("the socket write failed");
        });
        upstream.release(DONE_FRAME);
      },
      body: ONE_CONTENT_FRAME,
      endsResponse: true,
      terminal: "DONE",
      completed: true,
      votes: ["SUCCESS"],
      lines: [{ msg: "STREAM_TERMINAL_WRITE_FAILED", err_name: "Error" }],
    },
  ])(
    "runs the post-loop once with $row",
    async ({ act, body, endsResponse, terminal, completed, votes, lines }) => {
      const s = await committedStream();

      try {
        act(s);
        await until(
          () => s.capture.byEvent("stream_done").length > 0,
          `${CLIENT_STEP_RULE}: the post-loop runs and emits its terminal event`,
        );
        if (endsResponse) {
          await until(
            s.clientEnded,
            `${ENDED_RULE}: the client saw the response end`,
          );
        }
        for (let turn = 0; turn < AFTERMATH_TURNS; turn += 1) {
          await yieldToIo();
        }
        const warnings = s.capture.logs.filter(
          (log) => typeof log.level === "number" && log.level >= 40,
        );

        expect(
          s.capture.byEvent("stream_done"),
          `${CLIENT_STEP_RULE}: exactly one terminal event`,
        ).toHaveLength(1);
        expect(
          s.capture.byEvent("stream_done")[0],
          `${CLIENT_STEP_RULE}: the decided terminal stands`,
        ).toMatchObject({ terminal, completed });
        expect(
          s.received(),
          `${CLIENT_STEP_RULE}: the client sees only the bytes whose write succeeded`,
        ).toBe(body);
        expect(
          s.recorded,
          `${CLIENT_STEP_RULE}: the breaker sees one vote, the decided terminal's`,
        ).toStrictEqual(votes);
        expect(
          warnings.map(lineFields),
          `${CLIENT_STEP_RULE}: every absorbed failure leaves one line naming it, and nothing reaches the framework's error handler`,
        ).toStrictEqual(lines);
      } finally {
        await s.close();
      }
    },
  );
});
