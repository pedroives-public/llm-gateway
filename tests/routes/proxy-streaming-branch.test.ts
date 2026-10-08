import http from "node:http";
import { describe, it, expect, vi } from "vitest";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { makeLogCapture } from "../log-capture.js";
import {
  UPSTREAM_CLOSE_DEADLINE_MS,
  bufferedTripwire,
  streamingTripwire,
  withinDeadline,
} from "../helpers/streaming-seams.js";
import { isAcceptedStream } from "../../src/upstream/stream.js";
import type {
  AcceptedStream,
  StreamingAdapter,
} from "../../src/upstream/stream.js";
import { PARSER_BUFFER_CAP } from "../../src/upstream/sse-frame-reader.js";

// With the streaming flag ON, a `stream: true` attempt is decided by the
// upstream response HEAD alone: status plus the `content-type` media type.
// The real OpenAI client runs against a local fake so that closing the
// upstream body is observed from the fake's side of the socket.

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};

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

type HeldUpstream = {
  port: number;
  // Resolves with the response's `writableFinished` when its connection
  // closes: `false` means the connection died before the fake ended the body.
  closed: Promise<boolean>;
  close: () => Promise<void>;
};

// A fake upstream that sends the given head and the first bytes of a body,
// then never ends it. Only the gateway cancelling its read can close the
// connection, so the close is evidence of the cancel; a fake that ended the
// body would report the same close with or without one.
async function holdOpenUpstream(
  status: number,
  headers: Record<string, string>,
  firstBytes: string,
): Promise<HeldUpstream> {
  let reportClose: (finished: boolean) => void = () => {};
  const closed = new Promise<boolean>((resolve) => {
    reportClose = resolve;
  });

  const server = http.createServer((req, res) => {
    res.on("error", () => {});
    res.on("close", () => {
      reportClose(res.writableFinished);
    });
    req.resume();
    req.on("end", () => {
      res.writeHead(status, headers);
      // Send the head now: for a 204 the server drops body writes, and
      // without an explicit flush its head would never leave.
      res.flushHeaders();
      res.write(firstBytes);
    });
  });
  const { port, close } = await listenEphemeral(server);

  return {
    port,
    closed,
    close: async () => {
      // A cell that fails leaves the held response open; close() would wait
      // for it forever.
      server.closeAllConnections();
      await close();
    },
  };
}

type FakeUpstream = {
  port: number;
  close: () => Promise<void>;
  // Present only on a fake that holds its body open: see HeldUpstream.
  closed?: Promise<boolean>;
};

// A fake upstream that sends the given head and body and then ENDS the body:
// what the gateway reads after the head is a clean end of stream.
async function endingUpstream(
  status: number,
  headers: Record<string, string>,
  body: string,
): Promise<FakeUpstream> {
  const server = http.createServer((req, res) => {
    res.on("error", () => {});
    req.resume();
    req.on("end", () => {
      res.writeHead(status, headers);
      res.end(body);
    });
  });
  return listenEphemeral(server);
}

// Loopback delivers a response head within a few milliseconds; 50 ms leaves
// it time to reach the gateway before the connection dies (measured
// 2026-10-07: with 50 ms the failure landed on the body read in every run).
const RESET_AFTER_HEAD_MS = 50;

// A fake upstream that sends the given head and then destroys the connection
// without ending the body: the gateway's pending read rejects with a transport
// error instead of seeing an end of stream.
async function resettingUpstream(
  status: number,
  headers: Record<string, string>,
): Promise<FakeUpstream> {
  const server = http.createServer((req, res) => {
    res.on("error", () => {});
    req.resume();
    req.on("end", () => {
      res.writeHead(status, headers);
      res.flushHeaders();
      setTimeout(() => res.destroy(), RESET_AFTER_HEAD_MS);
    });
  });
  const { port, close } = await listenEphemeral(server);
  return {
    port,
    close: async () => {
      server.closeAllConnections();
      await close();
    },
  };
}

describe("streaming flag ON: the response head decides a stream: true attempt", () => {
  it("answers a 2xx that is not SSE as undecodable, cancels the upstream body, and never reaches the buffered read", async () => {
    // What a provider that ignores the `stream` field sends: a JSON completion.
    const upstream = await holdOpenUpstream(
      200,
      { "content-type": "application/json" },
      '{"id":"chatcmpl-x","choices":[',
    );
    const client = createOpenAIClient({
      apiKey: "gateway-key",
      baseURL: `http://127.0.0.1:${upstream.port}`,
    });
    let streamingCalls = 0;
    const violations: string[] = [];
    const { breaker, recorded } = recordingBreaker();
    const app = await buildProxyApp({
      breaker,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: (body, signal, log) => {
        streamingCalls += 1;
        return client.streaming(body, signal, log);
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      expect(violations).toStrictEqual([]);
      expect(res.statusCode).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe("upstream-fault");
      expect(res.headers["content-type"]).toBe(
        "application/json; charset=utf-8",
      );
      expect(res.json()).toStrictEqual({
        error: {
          message: "invalid response from upstream",
          type: "server_error",
          code: "upstream_decode_error",
        },
      });
      expect(streamingCalls).toBe(1);
      expect(recorded).toStrictEqual(["FAILURE"]);

      const finished = await withinDeadline(
        upstream.closed,
        UPSTREAM_CLOSE_DEADLINE_MS,
        "the upstream body was not cancelled: the fake saw no close before the deadline",
      );
      expect(
        finished,
        "the upstream connection closed only after the fake ended the body, not by a cancel",
      ).toBe(false);
    } finally {
      await app.close();
      await upstream.close();
    }
  });

  it("closes an accepted stream's body even when the breaker vote throws", async () => {
    // The route depends on the CircuitBreaker interface, not on today's
    // implementation, which never throws on INCONCLUSIVE; closing the opened
    // body must not rely on that.
    const upstream = await holdOpenUpstream(
      200,
      { "content-type": "text/event-stream" },
      'data: {"id":"chatcmpl-x","choices":[{"delta":{"content":"hi"}}]}\n\n',
    );
    const client = createOpenAIClient({
      apiKey: "gateway-key",
      baseURL: `http://127.0.0.1:${upstream.port}`,
    });
    const throwingVoteBreaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (outcome) => {
        if (outcome === "INCONCLUSIVE") {
          throw new Error("breaker vote failed");
        }
      },
      getState: () => "CLOSED",
    };
    const violations: string[] = [];
    const app = await buildProxyApp({
      breaker: throwingVoteBreaker,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: client.streaming,
    });

    try {
      await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      expect(violations).toStrictEqual([]);
      const finished = await withinDeadline(
        upstream.closed,
        UPSTREAM_CLOSE_DEADLINE_MS,
        "the accepted stream's body stayed open after the breaker vote threw: closing it depends on the vote succeeding",
      );
      expect(
        finished,
        "the upstream connection closed only after the fake ended the body, not by a cancel",
      ).toBe(false);
    } finally {
      await app.close();
      await upstream.close();
    }
  });

  it("names an accepted stream that was not delivered in its req_complete line", async () => {
    const upstream = await holdOpenUpstream(
      200,
      { "content-type": "text/event-stream" },
      'data: {"id":"chatcmpl-x","choices":[{"delta":{"content":"hi"}}]}\n\n',
    );
    const client = createOpenAIClient({
      apiKey: "gateway-key",
      baseURL: `http://127.0.0.1:${upstream.port}`,
    });
    const capture = makeLogCapture();
    const violations: string[] = [];
    const app = await buildProxyApp({
      breaker: stubBreaker,
      logger: capture.logger,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: client.streaming,
    });

    try {
      await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      expect(violations).toStrictEqual([]);
      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(
        complete[0],
        "req_complete must name the transitional terminal, so an operator can tell an undelivered stream from any other 500",
      ).toMatchObject({
        status: 500,
        error_class: "gateway-fault",
        stream: true,
        attempts: 1,
        retry_disposition: "ineligible",
        terminal: "STREAM_NOT_DELIVERED",
      });
    } finally {
      await app.close();
      await upstream.close();
    }
  });
});

describe("streaming flag ON: the fork reads the stream value Ajv coerced", () => {
  // Fastify's default Ajv coerces body types in place, so "true" and 1 arrive
  // at the handler as `true`, and "false" and 0 as `false`. The "true" row is
  // the one that proves the fork reads the coerced value: the raw string is
  // not `true`. With the flag OFF, the coercion cells in
  // tests/routes/proxy-stream-breaker.test.ts keep their answers.
  it.each([
    { stream: "true", calls: { streaming: 1, buffered: 0 } },
    { stream: 1, calls: { streaming: 1, buffered: 0 } },
    { stream: "false", calls: { streaming: 0, buffered: 1 } },
    { stream: 0, calls: { streaming: 0, buffered: 1 } },
  ])("stream: $stream reaches only its own seam", async ({ stream, calls }) => {
    const violations: string[] = [];
    const buffered = bufferedTripwire(violations);
    const streaming = streamingTripwire(violations);
    const app = await buildProxyApp({
      breaker: stubBreaker,
      streamingEnabled: true,
      upstreamBuffered: buffered.seam,
      upstreamStreaming: streaming.seam,
    });

    try {
      await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream },
      });

      expect(violations).toStrictEqual([]);
      expect({
        streaming: streaming.calls(),
        buffered: buffered.calls(),
      }).toStrictEqual(calls);
    } finally {
      await app.close();
    }
  });
});

// A fake upstream that answers every request with the given status and JSON
// body, ends it, and counts the requests it received: the count is the number
// of upstream calls, observed from the upstream's side.
async function answeringUpstream(
  status: number,
  body: string,
): Promise<{
  port: number;
  requests: () => number;
  close: () => Promise<void>;
}> {
  let requests = 0;
  const server = http.createServer((req, res) => {
    requests += 1;
    res.on("error", () => {});
    req.resume();
    req.on("end", () => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(body);
    });
  });
  const { port, close } = await listenEphemeral(server);
  return { port, requests: () => requests, close };
}

describe("streaming flag ON: a 2xx head is SSE only by its media type", () => {
  const SSE_TERMINAL = {
    status: 500,
    errorClass: "gateway-fault",
    code: "stream_not_delivered",
    votes: ["INCONCLUSIVE"],
  };
  const UNDECODABLE_TERMINAL = {
    status: 502,
    errorClass: "upstream-fault",
    code: "upstream_decode_error",
    votes: ["FAILURE"],
  };
  const SSE_TERMINAL_REASON =
    "a head recognized as SSE is not delivered in slice 1: the gap is the gateway's own, so 500 gateway-fault with an INCONCLUSIVE vote, never a 502 that blames the upstream";
  const UNDECODABLE_TERMINAL_REASON =
    "a 2xx that is not SSE breaks the upstream's own claim of success: 502 upstream-fault with a FAILURE vote";

  // Every row sends the same body bytes, a valid SSE frame; only the head
  // changes, so the head alone decides each answer. A 204 or 205 cannot carry
  // them: the server drops the bytes and fetch exposes no body at all.
  it.each([
    {
      status: 200,
      head: "text/event-stream",
      contentType: "text/event-stream",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because: "a 2xx head declaring text/event-stream is recognized as SSE",
    },
    {
      status: 200,
      head: "text/event-stream with a charset parameter",
      contentType: "text/event-stream; charset=utf-8",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because:
        "the comparison is by media type, the part before the first ';', so a parameter does not change the answer",
    },
    {
      status: 200,
      head: "text/event-stream in upper case",
      contentType: "TEXT/EVENT-STREAM",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because: "media types compare case-insensitively",
    },
    {
      status: 200,
      head: "application/json",
      contentType: "application/json",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      because: "a 2xx whose media type is not text/event-stream is undecodable",
    },
    {
      status: 200,
      head: "no content-type",
      contentType: undefined,
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      because:
        "a 2xx without content-type makes no SSE claim, so it is not SSE (fail-closed)",
    },
    {
      status: 201,
      head: "text/event-stream and a 2xx that is not 200",
      contentType: "text/event-stream",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because:
        "the 2xx filter is the range 200-299, as the official SDK's response.ok: the browser EventSource's 200-only rule binds the head the gateway sends its own client, not the upstream head it consumes as a fetch client",
    },
    {
      status: 204,
      head: "text/event-stream and a status that cannot carry a body",
      contentType: "text/event-stream",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      because:
        "a 2xx status that cannot carry a body can never carry a frame: it is the end-of-body-before-the-first-frame terminal, known at the head",
    },
    {
      status: 205,
      head: "text/event-stream and a status that cannot carry a body",
      contentType: "text/event-stream",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      because:
        "a 2xx status that cannot carry a body can never carry a frame: it is the end-of-body-before-the-first-frame terminal, known at the head",
    },
  ])(
    "$status with $head → $expected.code",
    async ({ status, contentType, expected, because, terminalReason }) => {
      const upstream = await holdOpenUpstream(
        status,
        contentType === undefined ? {} : { "content-type": contentType },
        'data: {"id":"chatcmpl-x","choices":[{"delta":{"content":"hi"}}]}\n\n',
      );
      const client = createOpenAIClient({
        apiKey: "gateway-key",
        baseURL: `http://127.0.0.1:${upstream.port}`,
      });
      const violations: string[] = [];
      const { breaker, recorded } = recordingBreaker();
      const app = await buildProxyApp({
        breaker,
        streamingEnabled: true,
        upstreamBuffered: bufferedTripwire(violations).seam,
        upstreamStreaming: client.streaming,
      });

      try {
        const res = await withinDeadline(
          app.inject({
            method: "POST",
            url: "/v1/chat/completions",
            headers: { authorization: bearer() },
            payload: { ...validBody, stream: true },
          }),
          UPSTREAM_CLOSE_DEADLINE_MS,
          // The upstream holds its body open, so a head misread as non-2xx
          // waits on that body and fails here: the row's reason goes in the
          // sentence, or this RED would never name the rule the row pins.
          `the head alone must decide the answer: the gateway sent no response before the deadline (${because})`,
        );

        expect(violations).toStrictEqual([]);
        const answer = {
          status: res.statusCode,
          errorClass: res.headers["x-gateway-error-class"],
          code: (res.json() as { error?: { code?: unknown } }).error?.code,
          votes: recorded,
        };
        // Two questions, two assertions: the verdict on the head carries the
        // row's reason, and the terminal that verdict leads to carries its own,
        // so a defect in one never fails naming the other.
        expect(answer.code, because).toBe(expected.code);
        expect(answer, terminalReason).toStrictEqual(expected);
      } finally {
        await app.close();
        await upstream.close();
      }
    },
  );
});

describe.each([
  { transport: "stream: true", stream: true },
  { transport: "stream: false", stream: false },
])(
  "streaming flag ON, $transport: a non-2xx head gets the buffered recognition",
  ({ stream }) => {
    // Twice the 1 MiB response cap, so the cap fires whatever the chunking.
    const OVER_CAP_BODY_BYTES = 2 * 1024 * 1024;

    // One literal answer per upstream head, identical for both transports:
    // the streaming attempt reaches the same recognition as the buffered one.
    // The over-cap row pins the path of a read that ends before recognition:
    // it must pass through as the capped terminal, never as a thrown error.
    it.each([
      {
        upstreamStatus: 500,
        upstreamBody: '{"error":{"type":"server_error"}}',
        expected: {
          status: 502,
          errorClass: "upstream-retry-exhausted",
          body: '{"error":{"type":"server_error"}}',
          upstreamCalls: 1,
          votes: ["FAILURE"],
        },
      },
      {
        upstreamStatus: 429,
        upstreamBody: '{"error":{"code":"rate_limit_exceeded"}}',
        expected: {
          status: 429,
          errorClass: "upstream-retry-exhausted",
          body: '{"error":{"code":"rate_limit_exceeded"}}',
          upstreamCalls: 1,
          votes: ["INCONCLUSIVE"],
        },
      },
      {
        upstreamStatus: 400,
        upstreamBody: '{"error":{"code":"invalid_value"}}',
        expected: {
          status: 400,
          errorClass: "client-fault",
          body: '{"error":{"code":"invalid_value"}}',
          upstreamCalls: 1,
          votes: ["INCONCLUSIVE"],
        },
      },
      {
        upstreamStatus: 500,
        upstreamBody: "x".repeat(OVER_CAP_BODY_BYTES),
        expected: {
          status: 502,
          errorClass: "upstream-fault",
          body: '{"error":{"message":"upstream response too large","type":"server_error","code":"response_too_large"}}',
          upstreamCalls: 1,
          votes: ["INCONCLUSIVE"],
        },
      },
    ])(
      "upstream $upstreamStatus → $expected.status $expected.errorClass",
      async ({ upstreamStatus, upstreamBody, expected }) => {
        const upstream = await answeringUpstream(upstreamStatus, upstreamBody);
        const client = createOpenAIClient({
          apiKey: "gateway-key",
          baseURL: `http://127.0.0.1:${upstream.port}`,
        });
        const violations: string[] = [];
        const { breaker, recorded } = recordingBreaker();
        const app = await buildProxyApp({
          breaker,
          streamingEnabled: true,
          upstreamBuffered: bufferedTripwire(violations, client.buffered).seam,
          upstreamStreaming: client.streaming,
        });

        try {
          const res = await app.inject({
            method: "POST",
            url: "/v1/chat/completions",
            headers: { authorization: bearer() },
            payload: { ...validBody, stream },
          });

          expect(violations).toStrictEqual([]);
          expect(
            {
              status: res.statusCode,
              errorClass: res.headers["x-gateway-error-class"],
              body: res.body,
              upstreamCalls: upstream.requests(),
              votes: recorded,
            },
            "a non-2xx head is recognized exactly as on the buffered path: same status, error class, body, upstream call count and vote",
          ).toStrictEqual(expected);
        } finally {
          await app.close();
          await upstream.close();
        }
      },
    );
  },
);

describe("streaming flag ON: the total-duration deadline bounds a stream: true request", () => {
  it("ends a silent upstream as TOTAL_TIMEOUT at the deadline, not at the 30-second non-streaming deadline", async () => {
    let calls = 0;
    const capture = makeLogCapture();
    const violations: string[] = [];

    const { breaker, recorded } = recordingBreaker();
    const app = await buildProxyApp({
      breaker: breaker,
      logger: capture.logger,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: (_body, signal) => {
        calls += 1;
        return new Promise((resolve) => {
          signal.addEventListener(
            "abort",
            () => resolve({ kind: "aborted", abort_kind: signal.reason.kind }),
            { once: true },
          );
        });
      },
    });

    vi.useFakeTimers();

    try {
      const req = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      await vi.advanceTimersByTimeAsync(30_000);
      expect(violations).toStrictEqual([]);
      expect(
        calls,
        "the request must be in flight at 30 seconds: the streaming upstream was called once and has not answered",
      ).toBe(1);
      expect(
        recorded,
        "a stream: true request is bounded by the 280-second total-duration deadline: the 30-second deadline of the non-streaming path must not end it",
      ).toStrictEqual([]);

      await vi.advanceTimersByTimeAsync(250_000);
      const res = await req;
      expect(
        res.statusCode,
        "the total-duration deadline expired before the first frame: the request must end as TOTAL_TIMEOUT, a 504",
      ).toBe(504);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(
        res.json(),
        "the pre-first-frame 504 of the total-duration deadline carries code total_timeout_exceeded",
      ).toStrictEqual({
        error: {
          message: "stream exceeded the total duration limit",
          type: "gateway_timeout",
          code: "total_timeout_exceeded",
        },
      });
      expect(
        recorded,
        "an expired deadline proves nothing about the upstream's health: exactly one INCONCLUSIVE breaker result",
      ).toStrictEqual(["INCONCLUSIVE"]);

      expect(
        capture.logs.flatMap((log) => log.event ?? []),
        "a pre-first-frame terminal emits req_complete, never stream_done: the request logs req_start and req_complete, and no other event",
      ).toStrictEqual(["req_start", "req_complete"]);
      const complete = capture.byEvent("req_complete");
      expect(
        complete[0],
        "a pre-first-frame terminal is named in req_complete: terminal TOTAL_TIMEOUT",
      ).toMatchObject({
        status: 504,
        error_class: "gateway-fault",
        stream: true,
        attempts: 1,
        retry_disposition: "ineligible",
        terminal: "TOTAL_TIMEOUT",
      });
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });

  // Two instants on the same scene. The first sits on the deadline itself: the
  // re-read compares with >=, so a clock that has reached the deadline is past
  // it. Only that row can tell >= from >.
  it.each([
    {
      when: "exactly at the deadline",
      clockAfterStartMs: 280_000,
      rule: "the deadline comparison is >=: a clock exactly at the deadline has reached it, so the request ends as TOTAL_TIMEOUT, a 504, whatever the upstream answered",
    },
    {
      when: "20 ms past the deadline",
      clockAfterStartMs: 280_020,
      rule: "the deadline is a value read at the terminal decision: past the deadline the request ends as TOTAL_TIMEOUT, a 504, whatever the upstream answered",
    },
  ])(
    "ends as TOTAL_TIMEOUT when the clock is $when, its timer has not fired and the upstream answers 502",
    async ({ clockAfterStartMs, rule }) => {
      let calls = 0;
      let resolveUpstream: ((result: StreamingAdapter) => void) | undefined;
      let receivedSignal: AbortSignal | undefined;

      const capture = makeLogCapture();
      const violations: string[] = [];
      const { breaker, recorded } = recordingBreaker();

      const app = await buildProxyApp({
        breaker: breaker,
        logger: capture.logger,
        streamingEnabled: true,
        upstreamBuffered: bufferedTripwire(violations).seam,
        upstreamStreaming: (_body, signal) => {
          calls += 1;
          receivedSignal = signal;

          return new Promise<StreamingAdapter>((resolve) => {
            resolveUpstream = resolve;
          });
        },
      });

      vi.useFakeTimers();
      const startedAt = Date.now();

      try {
        const req = app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: true },
        });

        await vi.advanceTimersByTimeAsync(1_000);

        expect(violations).toStrictEqual([]);
        expect(
          calls,
          "the scene's premise: the request is in flight, the streaming upstream was called once and has not answered",
        ).toBe(1);
        expect(receivedSignal).toBeDefined();
        expect(
          receivedSignal?.aborted,
          "the scene's premise: the deadline's timer has not fired, so the request signal is not aborted",
        ).toBe(false);

        const pendingTimers = vi.getTimerCount();
        expect(
          pendingTimers,
          "the scene's premise: the deadline's timer is still armed",
        ).toBe(1);

        vi.setSystemTime(startedAt + clockAfterStartMs);

        if (resolveUpstream === undefined) {
          throw new Error("resolveUpstream is undefined");
        }

        resolveUpstream({
          kind: "upstream_error",
          status: 502,
          body_raw: "upstream failure",
        });

        await vi.advanceTimersByTimeAsync(0);

        const res = await req;
        expect(res.statusCode, rule).toBe(504);
        expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
        expect(
          res.json(),
          "the deadline is a value read at the terminal decision: past the deadline the client gets the deadline's own 504 body, not the upstream's 502",
        ).toStrictEqual({
          error: {
            message: "stream exceeded the total duration limit",
            type: "gateway_timeout",
            code: "total_timeout_exceeded",
          },
        });
        expect(
          recorded,
          "the deadline is a value read at the terminal decision: a 502 that arrives past the deadline is not evidence about the upstream, so the breaker result is INCONCLUSIVE",
        ).toStrictEqual(["INCONCLUSIVE"]);

        expect(
          capture.logs.flatMap((log) => log.event ?? []),
          "a pre-first-frame terminal emits req_complete, never stream_done: the request logs req_start and req_complete, and no other event",
        ).toStrictEqual(["req_start", "req_complete"]);
        const complete = capture.byEvent("req_complete");
        expect(
          complete[0],
          "the deadline is a value read at the terminal decision: req_complete names TOTAL_TIMEOUT, not the upstream's error",
        ).toMatchObject({
          status: 504,
          error_class: "gateway-fault",
          stream: true,
          attempts: 1,
          retry_disposition: "ineligible",
          terminal: "TOTAL_TIMEOUT",
        });
      } finally {
        vi.useRealTimers();
        await app.close();
      }
    },
  );

  it("ends with the upstream's own 502 when it arrives after 30 seconds and before the deadline", async () => {
    let calls = 0;
    let resolveUpstream: ((result: StreamingAdapter) => void) | undefined;
    let receivedSignal: AbortSignal | undefined;

    const capture = makeLogCapture();
    const violations: string[] = [];
    const { breaker, recorded } = recordingBreaker();

    const app = await buildProxyApp({
      breaker: breaker,
      logger: capture.logger,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: (_body, signal) => {
        calls += 1;
        receivedSignal = signal;

        return new Promise<StreamingAdapter>((resolve) => {
          resolveUpstream = resolve;
        });
      },
    });

    vi.useFakeTimers();

    try {
      const req = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      await vi.advanceTimersByTimeAsync(1_000);
      await vi.advanceTimersByTimeAsync(99_000);

      expect(violations).toStrictEqual([]);
      expect(
        calls,
        "the scene's premise: the request is in flight, the streaming upstream was called once and has not answered",
      ).toBe(1);
      expect(receivedSignal).toBeDefined();
      expect(
        receivedSignal?.aborted,
        "the scene's premise: the deadline's timer has not fired, so the request signal is not aborted",
      ).toBe(false);

      const pendingTimers = vi.getTimerCount();
      expect(
        pendingTimers,
        "the scene's premise: the deadline's timer is still armed",
      ).toBe(1);

      if (resolveUpstream === undefined) {
        throw new Error("resolveUpstream is undefined");
      }

      resolveUpstream({
        kind: "upstream_error",
        status: 502,
        body_raw: '{"error":{"type":"server_error"}}',
      });

      await vi.advanceTimersByTimeAsync(0);
      const res = await req;
      expect(
        res.statusCode,
        "an upstream answer that arrives before the deadline decides the terminal: a 502 at 100 seconds is the upstream's 502, not a TOTAL_TIMEOUT",
      ).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(
        res.body,
        "before the deadline the upstream's error body reaches the client as it came",
      ).toBe('{"error":{"type":"server_error"}}');
      expect(
        recorded,
        "a 5xx that arrives before the deadline is evidence about the upstream: the breaker result is FAILURE",
      ).toStrictEqual(["FAILURE"]);

      // The request is over at 100 seconds. Moving past the deadline shows
      // whether its timer was disarmed: a timer left armed fires at 280
      // seconds and aborts the signal of a finished request.
      await vi.advanceTimersByTimeAsync(200_000);
      expect(
        receivedSignal?.aborted,
        "the deadline's timer is disarmed once the attempt loop returns: past the deadline the signal of a finished request is not aborted",
      ).toBe(false);

      const complete = capture.byEvent("req_complete");
      expect(
        complete,
        "the request logs exactly one req_complete line",
      ).toHaveLength(1);
      expect(
        complete[0],
        "before the deadline, req_complete describes the upstream's error",
      ).toMatchObject({
        status: 502,
        error_class: "upstream-retry-exhausted",
        stream: true,
        attempts: 1,
        retry_disposition: "ineligible",
      });
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });

  it.each<{ returns: string; returned: StreamingAdapter }>([
    {
      returns: "the deadline's own abort",
      returned: { kind: "aborted", abort_kind: "total_timeout" },
    },
    {
      returns: "an upstream error",
      returned: {
        kind: "upstream_error",
        status: 502,
        body_raw: "upstream failure",
      },
    },
  ])(
    "ends as TOTAL_TIMEOUT when the clock steps back after the deadline's timer has aborted the request and the attempt returns $returns",
    async ({ returned }) => {
      let calls = 0;
      let resolveUpstream: ((result: StreamingAdapter) => void) | undefined;
      let receivedSignal: AbortSignal | undefined;

      const capture = makeLogCapture();
      const violations: string[] = [];
      const { breaker, recorded } = recordingBreaker();

      const app = await buildProxyApp({
        breaker: breaker,
        logger: capture.logger,
        streamingEnabled: true,
        upstreamBuffered: bufferedTripwire(violations).seam,
        upstreamStreaming: (_body, signal) => {
          calls += 1;
          receivedSignal = signal;

          return new Promise<StreamingAdapter>((resolve) => {
            resolveUpstream = resolve;
          });
        },
      });

      vi.useFakeTimers();
      const startedAt = Date.now();

      try {
        const req = app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: true },
        });

        await vi.advanceTimersByTimeAsync(280_000);

        expect(violations).toStrictEqual([]);
        expect(
          calls,
          "the scene's premise: the streaming upstream was called once and has not answered",
        ).toBe(1);
        expect(receivedSignal).toBeDefined();
        expect(
          receivedSignal?.aborted,
          "the scene's premise: the deadline's timer has fired and aborted the request signal",
        ).toBe(true);
        expect(
          receivedSignal?.reason.kind,
          "the scene's premise: the abort reason is the deadline's own",
        ).toBe("total_timeout");

        const pendingTimers = vi.getTimerCount();
        expect(
          pendingTimers,
          "the scene's premise: the deadline's timer has fired and no other timer is armed",
        ).toBe(0);

        vi.setSystemTime(startedAt + 279_990);

        if (resolveUpstream === undefined) {
          throw new Error("resolveUpstream is undefined");
        }

        resolveUpstream(returned);

        await vi.advanceTimersByTimeAsync(0);

        const res = await req;
        expect(
          res.statusCode,
          "once the deadline's timer has aborted the request the terminal is TOTAL_TIMEOUT, whatever the attempt returned and even when the clock has stepped back behind the deadline: the request ends with a 504",
        ).toBe(504);
        expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
        expect(
          res.json(),
          "once the deadline's timer has aborted the request the terminal is TOTAL_TIMEOUT, whatever the attempt returned: the client gets the same 504 body as at the deadline",
        ).toStrictEqual({
          error: {
            message: "stream exceeded the total duration limit",
            type: "gateway_timeout",
            code: "total_timeout_exceeded",
          },
        });
        expect(
          recorded,
          "once the deadline's timer has aborted the request the terminal is TOTAL_TIMEOUT, whatever the attempt returned: one INCONCLUSIVE breaker result",
        ).toStrictEqual(["INCONCLUSIVE"]);

        expect(
          capture.logs.flatMap((log) => log.event ?? []),
          "a pre-first-frame terminal emits req_complete, never stream_done: the request logs req_start and req_complete, and no other event",
        ).toStrictEqual(["req_start", "req_complete"]);
        const complete = capture.byEvent("req_complete");
        expect(
          complete[0],
          "once the deadline's timer has aborted the request the terminal is TOTAL_TIMEOUT, whatever the attempt returned: req_complete names the terminal",
        ).toMatchObject({
          status: 504,
          error_class: "gateway-fault",
          stream: true,
          attempts: 1,
          retry_disposition: "ineligible",
          terminal: "TOTAL_TIMEOUT",
        });
      } finally {
        vi.useRealTimers();
        await app.close();
      }
    },
  );
});

describe("streaming flag ON: the first frame is checked before anything is written", () => {
  it.each([
    {
      when: "it is the first frame of the body",
      firstBytes: "data: not-json\n\n",
    },
    {
      when: "a comment-only frame comes before it",
      firstBytes: ": keep-alive\n\ndata: not-json\n\n",
    },
    {
      when: "two bare empty lines come before it",
      firstBytes: "\n\ndata: not-json\n\n",
    },
  ])(
    "answers a first data frame that is not JSON as undecodable, cancels the upstream body, and writes no stream, when $when",
    async ({ firstBytes }) => {
      const upstream = await holdOpenUpstream(
        200,
        { "content-type": "text/event-stream" },
        firstBytes,
      );
      const client = createOpenAIClient({
        apiKey: "gateway-key",
        baseURL: `http://127.0.0.1:${upstream.port}`,
      });
      const capture = makeLogCapture();
      const violations: string[] = [];
      const { breaker, recorded } = recordingBreaker();
      const app = await buildProxyApp({
        breaker,
        logger: capture.logger,
        streamingEnabled: true,
        upstreamBuffered: bufferedTripwire(violations).seam,
        upstreamStreaming: client.streaming,
      });

      try {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: true },
        });

        expect(violations).toStrictEqual([]);
        expect(
          res.statusCode,
          "the first frame that carries data is not JSON, and frames without data before it do not count as the first frame: the request ends with a clean 502, never a 200 stream",
        ).toBe(502);
        expect(
          res.headers["x-gateway-error-class"],
          "a first frame whose data is not JSON is a decode failure of a real upstream attempt: the class is upstream-fault",
        ).toBe("upstream-fault");
        expect(
          res.headers["content-type"],
          "no SSE head is written when the first frame fails the JSON check: the error is a JSON body",
        ).toBe("application/json; charset=utf-8");
        expect(
          res.json(),
          "a first frame whose data is not JSON gets the same client-facing error as any undecodable upstream response",
        ).toStrictEqual({
          error: {
            message: "invalid response from upstream",
            type: "server_error",
            code: "upstream_decode_error",
          },
        });
        expect(
          recorded,
          "a first frame whose data is not JSON is a decode failure of a real upstream attempt: one FAILURE breaker result",
        ).toStrictEqual(["FAILURE"]);

        const finished = await withinDeadline(
          upstream.closed,
          UPSTREAM_CLOSE_DEADLINE_MS,
          "the upstream body was not cancelled after the first frame failed the JSON check: the fake saw no close before the deadline",
        );
        expect(
          finished,
          "the upstream connection closed only after the fake ended the body, not by a cancel",
        ).toBe(false);

        expect(
          capture.logs.flatMap((log) => log.event ?? []),
          "a pre-first-frame terminal emits req_complete, never stream_done: the request logs req_start and req_complete, and no other event",
        ).toStrictEqual(["req_start", "req_complete"]);
        const complete = capture.byEvent("req_complete");
        expect(
          complete[0],
          "req_complete names the terminal, so an operator can tell a first frame that is not JSON from any other undecodable upstream response",
        ).toMatchObject({
          status: 502,
          error_class: "upstream-fault",
          stream: true,
          attempts: 1,
          retry_disposition: "ineligible",
          terminal: "FIRST_FRAME_NOT_JSON",
        });
      } finally {
        await app.close();
        await upstream.close();
      }
    },
  );

  const SSE_HEAD = { "content-type": "text/event-stream" };
  const UNDECODABLE_BODY = {
    error: {
      message: "invalid response from upstream",
      type: "server_error",
      code: "upstream_decode_error",
    },
  };
  const CONNECTION_FAILED_BODY = {
    error: {
      message: "gateway error",
      type: "gateway_error",
      code: "upstream_connection_failed",
    },
  };
  type ExpectedTerminal = {
    status: number;
    errorClass: string;
    body: { error: { message: string; type: string; code: string } };
    votes: ProbeOutcome[];
    terminal: string;
  };
  const EOF_BEFORE_FIRST_FRAME: ExpectedTerminal = {
    status: 502,
    errorClass: "upstream-fault",
    body: UNDECODABLE_BODY,
    votes: ["FAILURE"],
    terminal: "UPSTREAM_EOF_BEFORE_FIRST_FRAME",
  };
  const EOF_RULE =
    "a body that ends before a first frame with data is never a clean completion: 502 upstream-fault with a FAILURE vote, and nothing written as a stream";

  it.each<{
    when: string;
    makeUpstream: () => Promise<FakeUpstream>;
    expected: ExpectedTerminal;
    rule: string;
  }>([
    {
      when: "the body ends with no bytes",
      makeUpstream: () => endingUpstream(200, SSE_HEAD, ""),
      expected: EOF_BEFORE_FIRST_FRAME,
      rule: EOF_RULE,
    },
    {
      when: "the body ends with a partial frame",
      makeUpstream: () => endingUpstream(200, SSE_HEAD, "data: partial"),
      expected: EOF_BEFORE_FIRST_FRAME,
      rule: EOF_RULE,
    },
    {
      when: "the body ends after only a comment frame",
      makeUpstream: () => endingUpstream(200, SSE_HEAD, ": keep-alive\n\n"),
      expected: EOF_BEFORE_FIRST_FRAME,
      rule: EOF_RULE,
    },
    {
      when: "the bytes pass the frame buffer cap without forming a frame",
      makeUpstream: () =>
        holdOpenUpstream(
          200,
          SSE_HEAD,
          `data: ${"x".repeat(PARSER_BUFFER_CAP + 1)}`,
        ),
      expected: {
        status: 502,
        errorClass: "upstream-fault",
        body: UNDECODABLE_BODY,
        votes: ["FAILURE"],
        terminal: "PARSER_BUFFER_CAP",
      },
      rule: "bytes that pass the frame buffer cap before a first frame never form one: 502 upstream-fault with a FAILURE vote, and nothing written as a stream",
    },
    {
      when: "the connection is destroyed after the head",
      makeUpstream: () => resettingUpstream(200, SSE_HEAD),
      expected: {
        status: 504,
        errorClass: "gateway-fault",
        body: CONNECTION_FAILED_BODY,
        votes: ["FAILURE"],
        terminal: "NETWORK_FAILED_POST_SEND",
      },
      rule: "a connection that dies after the head and before a first frame is a network failure after the request was sent: 504 gateway-fault with exactly one FAILURE vote",
    },
  ])(
    "ends as $expected.terminal when $when, before anything is written",
    async ({ makeUpstream, expected, rule }) => {
      const upstream = await makeUpstream();
      const client = createOpenAIClient({
        apiKey: "gateway-key",
        baseURL: `http://127.0.0.1:${upstream.port}`,
      });
      const capture = makeLogCapture();
      const violations: string[] = [];
      const accepted: boolean[] = [];
      const { breaker, recorded } = recordingBreaker();
      const app = await buildProxyApp({
        breaker,
        logger: capture.logger,
        streamingEnabled: true,
        upstreamBuffered: bufferedTripwire(violations).seam,
        upstreamStreaming: async (body, signal, log) => {
          const result = await client.streaming(body, signal, log);
          accepted.push(isAcceptedStream(result));
          return result;
        },
      });

      try {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: true },
        });

        expect(violations).toStrictEqual([]);
        expect(
          accepted,
          "the scene's premise: the upstream head arrived and was accepted as a stream, so what follows is decided by the first-frame read",
        ).toStrictEqual([true]);
        expect(
          {
            status: res.statusCode,
            errorClass: res.headers["x-gateway-error-class"],
            contentType: res.headers["content-type"],
            body: res.json(),
            votes: recorded,
          },
          rule,
        ).toStrictEqual({
          status: expected.status,
          errorClass: expected.errorClass,
          contentType: "application/json; charset=utf-8",
          body: expected.body,
          votes: expected.votes,
        });

        if (upstream.closed !== undefined) {
          const finished = await withinDeadline(
            upstream.closed,
            UPSTREAM_CLOSE_DEADLINE_MS,
            "the upstream body was not cancelled after the first-frame read ended the request: the fake saw no close before the deadline",
          );
          expect(
            finished,
            "the upstream connection closed only after the fake ended the body, not by a cancel",
          ).toBe(false);
        }

        expect(
          capture.logs.flatMap((log) => log.event ?? []),
          "a pre-first-frame terminal emits req_complete, never stream_done: the request logs req_start and req_complete, and no other event",
        ).toStrictEqual(["req_start", "req_complete"]);
        expect(
          capture.byEvent("req_complete")[0],
          "req_complete names the terminal, so an operator can tell which pre-first-frame row ended the request",
        ).toMatchObject({
          status: expected.status,
          error_class: expected.errorClass,
          stream: true,
          attempts: 1,
          retry_disposition: "ineligible",
          terminal: expected.terminal,
        });
      } finally {
        await app.close();
        await upstream.close();
      }
    },
  );
});

describe("streaming flag ON: the total-duration deadline spans the first-frame wait", () => {
  const TOTAL_TIMEOUT_BODY = {
    error: {
      message: "stream exceeded the total duration limit",
      type: "gateway_timeout",
      code: "total_timeout_exceeded",
    },
  };

  // A fake accepted stream whose body never yields a byte. Its pending read
  // rejects with the request signal's own reason when that signal aborts, the
  // way an undici body does (measured 2026-10-01), so the route's catch sees
  // the deadline's abort by identity.
  function silentAcceptedStream(signal: AbortSignal): AcceptedStream {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        signal.addEventListener(
          "abort",
          () => {
            controller.error(signal.reason);
          },
          { once: true },
        );
      },
      pull() {
        return new Promise<void>(() => {});
      },
    });
    return { kind: "accepted_stream", status: 200, reader: body.getReader() };
  }

  it("ends an accepted head followed by silence as TOTAL_TIMEOUT when the deadline fires during the first-frame wait", async () => {
    let calls = 0;
    let receivedSignal: AbortSignal | undefined;

    const capture = makeLogCapture();
    const violations: string[] = [];
    const { breaker, recorded } = recordingBreaker();

    const app = await buildProxyApp({
      breaker,
      logger: capture.logger,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: (_body, signal) => {
        calls += 1;
        receivedSignal = signal;
        return Promise.resolve(silentAcceptedStream(signal));
      },
    });

    vi.useFakeTimers();

    try {
      const req = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      await vi.advanceTimersByTimeAsync(1_000);

      expect(violations).toStrictEqual([]);
      expect(
        calls,
        "the scene's premise: the streaming upstream was called once and answered with an accepted head",
      ).toBe(1);
      expect(receivedSignal).toBeDefined();
      expect(
        receivedSignal?.aborted,
        "the scene's premise: one second in, the deadline's timer has not fired",
      ).toBe(false);
      expect(
        vi.getTimerCount(),
        "the deadline's timer stays armed during the first-frame wait: an accepted head followed by silence is bounded by T, not by nothing",
      ).toBe(1);

      await vi.advanceTimersByTimeAsync(279_000);

      expect(
        receivedSignal?.reason?.kind,
        "the scene's premise: at T the deadline's timer aborted the request signal with its own reason",
      ).toBe("total_timeout");

      const res = await req;
      expect(
        res.statusCode,
        "the deadline fired during the first-frame wait: the request ends as TOTAL_TIMEOUT, a 504, with nothing written as a stream",
      ).toBe(504);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(
        res.json(),
        "the deadline fired during the first-frame wait: the client gets the deadline's own 504 body",
      ).toStrictEqual(TOTAL_TIMEOUT_BODY);
      expect(
        recorded,
        "a deadline that fires before the first frame is not evidence about the upstream: exactly one INCONCLUSIVE breaker result",
      ).toStrictEqual(["INCONCLUSIVE"]);

      expect(
        capture.logs.flatMap((log) => log.event ?? []),
        "the abort already errored the body, so the route's cancel on every exit is seen as the cancel-rejected observation between req_start and req_complete, and no stream_done",
      ).toStrictEqual(["req_start", "upstream_cancel_rejected", "req_complete"]);
      expect(
        capture.byEvent("upstream_cancel_rejected")[0],
        "the route, not the adapter, cancels the body it received still open",
      ).toMatchObject({ site: "proxy_route" });
      expect(
        capture.byEvent("req_complete")[0],
        "req_complete names the terminal: TOTAL_TIMEOUT, decided from the deadline condition read after the first-frame read returned",
      ).toMatchObject({
        status: 504,
        error_class: "gateway-fault",
        stream: true,
        attempts: 1,
        retry_disposition: "ineligible",
        terminal: "TOTAL_TIMEOUT",
      });
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });

});

describe("streaming flag ON: the first-frame line meets the total-duration deadline", () => {
  const TOTAL_TIMEOUT_BODY = {
    error: {
      message: "stream exceeded the total duration limit",
      type: "gateway_timeout",
      code: "total_timeout_exceeded",
    },
  };
  const JSON_FRAME = 'data: {"a":1}\n\n';
  const NOT_JSON_FRAME = "data: not-json\n\n";

  // A fake accepted stream the cell drives by hand. `release(text)` makes the
  // pending read resolve with those bytes; an abort of the request signal
  // errors the stream with the signal's reason, as an undici body does
  // (measured 2026-10-01); and with `holdCancel` the body's cancel() stays
  // pending until the cell calls `finishCancel()`, so a terminal's cleanup can
  // be held open while the deadline fires.
  function drivenAcceptedStream(
    signal: AbortSignal,
    holdCancel = false,
  ): {
    stream: AcceptedStream;
    release: (text: string) => void;
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

  function inject(app: Awaited<ReturnType<typeof buildProxyApp>>) {
    return app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer() },
      payload: { ...validBody, stream: true },
    });
  }

  it.each([
    {
      frame: "a frame that is valid JSON",
      bytes: JSON_FRAME,
      rule: "the deadline condition is read immediately before the write, never the order in which the frame and the firing arrived: a frame available with the clock exactly at the deadline and the firing late ends as TOTAL_TIMEOUT, a 504, with zero SSE bytes",
    },
    {
      frame: "a frame that is not JSON",
      bytes: NOT_JSON_FRAME,
      rule: "the deadline condition is read after the first-frame read returns and before any terminal is decided from it: a frame that is not JSON, available after the deadline with the firing late, ends as TOTAL_TIMEOUT, never FIRST_FRAME_NOT_JSON",
    },
  ])(
    "ends as TOTAL_TIMEOUT when $frame becomes available with the clock at the deadline and the firing late",
    async ({ bytes, rule }) => {
      const t = await buildDrivenApp();
      vi.useFakeTimers();
      const startedAt = Date.now();

      try {
        const req = inject(t.app);
        await vi.advanceTimersByTimeAsync(1_000);

        expect(t.violations).toStrictEqual([]);
        expect(
          t.calls(),
          "the scene's premise: the streaming upstream was called once and answered with an accepted head",
        ).toBe(1);
        expect(
          t.signal()?.aborted,
          "the scene's premise: the deadline's timer has not fired, so the request signal is not aborted",
        ).toBe(false);

        vi.setSystemTime(startedAt + 280_000);
        t.driven().release(bytes);
        await vi.advanceTimersByTimeAsync(0);

        const res = await req;
        expect(res.statusCode, rule).toBe(504);
        expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
        expect(
          res.headers["content-type"],
          "nothing is written as a stream once the deadline condition holds: the 504 is a JSON body, not an SSE head",
        ).toBe("application/json; charset=utf-8");
        expect(res.json(), rule).toStrictEqual(TOTAL_TIMEOUT_BODY);
        expect(
          t.recorded,
          "a frame that arrives past the deadline is not evidence about the upstream: exactly one INCONCLUSIVE breaker result",
        ).toStrictEqual(["INCONCLUSIVE"]);
        expect(
          t.driven().cancelCalled(),
          "the route cancels the body it received still open on every exit after the decision",
        ).toBe(true);
        expect(
          t.capture.logs.flatMap((log) => log.event ?? []),
          "a pre-first-frame terminal emits req_complete, never stream_done: the request logs req_start and req_complete, and no other event",
        ).toStrictEqual(["req_start", "req_complete"]);
        expect(t.capture.byEvent("req_complete")[0], rule).toMatchObject({
          status: 504,
          error_class: "gateway-fault",
          stream: true,
          attempts: 1,
          retry_disposition: "ineligible",
          terminal: "TOTAL_TIMEOUT",
        });
      } finally {
        vi.useRealTimers();
        await t.app.close();
      }
    },
  );

  it("ends as TOTAL_TIMEOUT when the deadline's timer has aborted, the clock has stepped back, and a frame read before the abort is available", async () => {
    const t = await buildDrivenApp();
    vi.useFakeTimers();
    const startedAt = Date.now();

    try {
      const req = inject(t.app);
      await vi.advanceTimersByTimeAsync(1_000);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.calls(),
        "the scene's premise: the streaming upstream was called once and answered with an accepted head",
      ).toBe(1);

      // The frame resolves the pending read; in the same tick, before the
      // route's continuation runs, the deadline's timer fires and aborts, and
      // the clock steps back behind the deadline value.
      t.driven().release(JSON_FRAME);
      vi.advanceTimersByTime(279_000);
      expect(
        t.signal()?.reason?.kind,
        "the scene's premise: the deadline's timer has fired and aborted the request signal with its own reason",
      ).toBe("total_timeout");
      vi.setSystemTime(startedAt + 279_990);
      await vi.advanceTimersByTimeAsync(0);

      const res = await req;
      expect(
        res.statusCode,
        "the deadline condition is the value reached OR the deadline's own timer having aborted: with the clock stepped back behind the value, the abort reason alone ends the request as TOTAL_TIMEOUT, a 504, and the frame is never written",
      ).toBe(504);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(res.json()).toStrictEqual(TOTAL_TIMEOUT_BODY);
      expect(
        t.recorded,
        "a frame read before the deadline's abort is not evidence about the upstream once the abort stands: exactly one INCONCLUSIVE breaker result",
      ).toStrictEqual(["INCONCLUSIVE"]);
      expect(
        t.capture.logs.flatMap((log) => log.event ?? []),
        "the abort already errored the body, so the route's cancel on every exit is seen as the cancel-rejected observation between req_start and req_complete, and no stream_done",
      ).toStrictEqual(["req_start", "upstream_cancel_rejected", "req_complete"]);
      expect(t.capture.byEvent("req_complete")[0]).toMatchObject({
        status: 504,
        error_class: "gateway-fault",
        stream: true,
        attempts: 1,
        retry_disposition: "ineligible",
        terminal: "TOTAL_TIMEOUT",
      });
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });

});

