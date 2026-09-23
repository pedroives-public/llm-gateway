import http from "node:http";
import { describe, it, expect } from "vitest";
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
): Promise<{ port: number; requests: () => number; close: () => Promise<void> }> {
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
  // changes, so the head alone decides each answer.
  it.each([
    {
      head: "text/event-stream",
      contentType: "text/event-stream",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because: "a 2xx head declaring text/event-stream is recognized as SSE",
    },
    {
      head: "text/event-stream with a charset parameter",
      contentType: "text/event-stream; charset=utf-8",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because:
        "the comparison is by media type, the part before the first ';', so a parameter does not change the answer",
    },
    {
      head: "text/event-stream in upper case",
      contentType: "TEXT/EVENT-STREAM",
      expected: SSE_TERMINAL,
      terminalReason: SSE_TERMINAL_REASON,
      because: "media types compare case-insensitively",
    },
    {
      head: "application/json",
      contentType: "application/json",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      because: "a 2xx whose media type is not text/event-stream is undecodable",
    },
    {
      head: "no content-type",
      contentType: undefined,
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      because:
        "a 2xx without content-type makes no SSE claim, so it is not SSE (fail-closed)",
    },
  ])(
    "200 with $head → $expected.code",
    async ({ contentType, expected, because, terminalReason }) => {
    const upstream = await holdOpenUpstream(
      200,
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
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

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
    // One literal answer per upstream head, identical for both transports:
    // the streaming attempt reaches the same recognition as the buffered one.
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
