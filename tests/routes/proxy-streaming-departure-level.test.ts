import http from "node:http";
import { describe, it, expect, vi } from "vitest";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { AcceptedStream } from "../../src/upstream/stream.js";
import { PARSER_BUFFER_CAP } from "../../src/upstream/sse-frame-reader.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { bufferedTripwire } from "../helpers/streaming-seams.js";
import { makeLogCapture } from "../log-capture.js";

// The loop reads the response's destroyed flag at every wake, because a
// destroy is visible at once while the departure observer's abort waits for
// the response's close event. Only that window lets the level read decide on
// its own, and a real socket cannot hold it open, so in this file the
// observer's abort is held back: the real total-duration helper runs with
// its client-departure capability replaced by a no-op. The mock is file-wide,
// so these rows live in their own file.
vi.mock("../../src/reliability/timeouts.js", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../src/reliability/timeouts.js")>();

  return {
    ...actual,
    armTotalDurationTimeout: (
      ...args: Parameters<typeof actual.armTotalDurationTimeout>
    ) => ({
      ...actual.armTotalDurationTimeout(...args),
      abortForClientGoneIfUndecided: () => {},
    }),
  };
});

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};
const ONE_CONTENT_FRAME =
  'data: {"id": "c1", "choices": [{"delta": {"content": "Hel"}}]}\n\n';

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

// An accepted stream whose frames the test releases by hand; an abort of the
// request signal errors it with the signal's reason, as an undici body does.
function drivenAcceptedStream(
  signal: AbortSignal,
  holdCancel = false,
): {
  stream: AcceptedStream;
  release: (text: string) => void;
  end: () => void;
  fail: (error: unknown) => void;
  finishCancel: () => void;
  cancelCalled: () => boolean;
} {
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  let finishCancel: (() => void) | undefined;
  const cancelHeld = new Promise<void>((resolve) => {
    finishCancel = resolve;
  });
  let cancelCalled = false;

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
    cancel() {
      cancelCalled = true;
      return holdCancel ? cancelHeld : Promise.resolve();
    },
  });

  return {
    stream: { kind: "accepted_stream", status: 200, reader: body.getReader() },
    release: (text) => {
      if (controller === undefined) {
        throw new Error("the fake stream has not started");
      }
      controller.enqueue(new TextEncoder().encode(text));
    },
    end: () => {
      if (controller === undefined) {
        throw new Error("the fake stream has not started");
      }
      controller.close();
    },
    fail: (error) => {
      if (controller === undefined) {
        throw new Error("the fake stream has not started");
      }
      controller.error(error);
    },
    finishCancel: () => {
      if (finishCancel === undefined) {
        throw new Error("the fake stream's cancel is not held");
      }
      finishCancel();
    },
    cancelCalled: () => cancelCalled,
  };
}

async function buildDrivenApp(holdCancel = false) {
  const capture = makeLogCapture();
  const violations: string[] = [];
  const { breaker, recorded } = recordingBreaker();
  let driven: ReturnType<typeof drivenAcceptedStream> | undefined;
  let receivedSignal: AbortSignal | undefined;
  let calls = 0;

  const app = await buildProxyApp({
    breaker,
    logger: capture.logger,
    streamingEnabled: true,
    upstreamBuffered: bufferedTripwire(violations).seam,
    upstreamStreaming: (_body, signal) => {
      calls += 1;
      receivedSignal = signal;
      driven = drivenAcceptedStream(signal, holdCancel);
      return Promise.resolve(driven.stream);
    },
  });

  return {
    app,
    capture,
    violations,
    recorded,
    calls: () => calls,
    signal: () => receivedSignal,
    driven: () => {
      if (driven === undefined) {
        throw new Error("the streaming upstream was not called");
      }
      return driven;
    },
  };
}

describe("streaming flag ON: after the first-frame line, a destroyed response is a client departure at every wake", () => {
  const LATE_FRAME =
    'data: {"id": "c1", "choices": [{"delta": {"content": "late"}}]}\n\n';
  const RESET_AFTER_DEPARTURE = Object.assign(new TypeError("terminated"), {
    cause: Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }),
  });
  const DEPARTURE_RULE = "a destroyed response is a client departure at this wake";
  const JURISDICTION_RULE =
    "the departure outranks the deadline by jurisdiction over the write";

  type Wake = {
    driven: ReturnType<typeof drivenAcceptedStream>;
    startedAt: number;
  };

  it.each([
    {
      wake: "an ordinary frame",
      act: ({ driven }: Wake) => driven.release(LATE_FRAME),
      rule: `${DEPARTURE_RULE}: an ordinary frame is never forwarded and the loop does not continue`,
    },
    {
      wake: "the terminating frame",
      act: ({ driven }: Wake) => driven.release("data: [DONE]\n\n"),
      rule: `${DEPARTURE_RULE}: the terminating frame never makes it DONE`,
    },
    {
      wake: "an eof between frames",
      act: ({ driven }: Wake) => driven.end(),
      rule: `${DEPARTURE_RULE}: an eof between frames never makes it EOF_WITHOUT_DONE`,
    },
    {
      wake: "an eof inside a frame",
      act: ({ driven }: Wake) => {
        driven.release('data: {"id": "c1", "choices": [{"delta": {"content": "w');
        driven.end();
      },
      rule: `${DEPARTURE_RULE}: an eof inside a frame never makes it EOF_WITHOUT_DONE`,
    },
    {
      wake: "the reader's cap",
      act: ({ driven }: Wake) =>
        driven.release(`data: ${"x".repeat(PARSER_BUFFER_CAP + 1)}`),
      rule: `${DEPARTURE_RULE}: the reader's cap never makes it PARSER_BUFFER_CAP`,
    },
    {
      wake: "a transport error",
      act: ({ driven }: Wake) => driven.fail(RESET_AFTER_DEPARTURE),
      rule: `${DEPARTURE_RULE}: a transport error never makes it UPSTREAM_DEATH`,
    },
    {
      wake: "the deadline's timer firing",
      act: () => vi.advanceTimersByTime(280_000),
      rule: `${JURISDICTION_RULE}: the deadline's own abort never makes it TOTAL_TIMEOUT`,
    },
    {
      wake: "a frame past the deadline, timer late",
      act: ({ driven, startedAt }: Wake) => {
        vi.setSystemTime(startedAt + 280_200);
        driven.release(LATE_FRAME);
      },
      rule: `${JURISDICTION_RULE}: a frame read past the deadline never makes it TOTAL_TIMEOUT`,
    },
  ])("ends as CLIENT_GONE and writes nothing when the loop wakes with $wake", async ({ act, rule }) => {
    const t = await buildDrivenApp();
    let serverResponse: http.ServerResponse | undefined;
    t.app.server.on("request", (_req, res: http.ServerResponse) => {
      serverResponse = res;
    });
    await t.app.listen({ port: 0, host: "127.0.0.1" });
    const address = t.app.server.address();
    if (address === null || typeof address === "string") {
      throw new Error("the app did not listen on a TCP port");
    }
    // setImmediate stays real so the test can yield to socket I/O while the
    // clock and the deadline's timer are fake.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const startedAt = Date.now();
    const yieldToIo = () =>
      new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    const until = async (ready: () => boolean, sentence: string) => {
      for (let turn = 0; turn < 200; turn += 1) {
        if (ready()) return;
        await yieldToIo();
      }
      throw new Error(sentence);
    };

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
    client.on("response", (res) => {
      res.on("data", (chunk: Buffer) => {
        received.push(chunk);
      });
    });

    try {
      client.end(body);
      await until(
        () => t.calls() === 1,
        "the scene's premise: the streaming upstream was called once and answered with an accepted head",
      );
      t.driven().release(ONE_CONTENT_FRAME);
      await until(
        () =>
          Buffer.concat(received).length >=
          Buffer.byteLength(ONE_CONTENT_FRAME),
        "the scene's premise: the client received the first frame, so the stream crossed the first-frame line",
      );
      expect(t.violations).toStrictEqual([]);

      client.destroy();
      await until(
        () => serverResponse?.destroyed === true,
        "the scene's premise: the server saw the client leave, so the response is destroyed before the loop wakes",
      );
      const response = serverResponse;
      if (response === undefined) {
        throw new Error("the server response was not captured");
      }
      expect(
        t.signal()?.aborted,
        "the scene's premise: the departure observer's abort is held back in this file, so nothing has aborted the request signal and only the destroyed response can decide",
      ).toBe(false);
      const writes = vi.spyOn(response, "write");
      const ends = vi.spyOn(response, "end");

      act({ driven: t.driven(), startedAt });
      await until(() => t.capture.byEvent("stream_done").length > 0, rule);

      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(t.capture.byEvent("stream_done")[0], rule).toMatchObject({
        completed: false,
        terminal: "CLIENT_GONE",
        error_class: "client-fault",
      });
      expect(
        writes.mock.calls,
        "nothing is written after the departure: no frame, no sentinel",
      ).toStrictEqual([]);
      expect(
        ends.mock.calls,
        "nothing is written after the departure: the response is already destroyed, so the gateway does not end it either",
      ).toStrictEqual([]);
      expect(
        t.recorded,
        "the departure votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
    } finally {
      client.destroy();
      vi.useRealTimers();
      await t.app.close();
    }
  });
});
