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
import { makeLogCapture, type LogCapture } from "../log-capture.js";
import {
  createAdmissionGate,
  type AdmissionGate,
} from "../../src/reliability/admission-gate.js";
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
    // implementation, which never throws on SUCCESS; closing the opened body
    // must not rely on that. The stream is the empty one, its only data frame
    // the terminating frame, so the response ends and the vote is SUCCESS.
    const upstream = await holdOpenUpstream(
      200,
      { "content-type": "text/event-stream" },
      "data: [DONE]\n\n",
    );
    const client = createOpenAIClient({
      apiKey: "gateway-key",
      baseURL: `http://127.0.0.1:${upstream.port}`,
    });
    const throwingVoteBreaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (outcome) => {
        if (outcome === "SUCCESS") {
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
  headers: Record<string, string> = {},
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
      res.writeHead(status, { "content-type": "application/json", ...headers });
      res.end(body);
    });
  });
  const { port, close } = await listenEphemeral(server);
  return { port, requests: () => requests, close };
}

describe("streaming flag ON: a 2xx head is SSE only by its media type", () => {
  const DELIVERED_EMPTY_STREAM = {
    kind: "delivered",
    label: "a delivered empty stream",
    status: 200,
    contentType: "text/event-stream",
    body: "data: [DONE]\n\n",
    votes: ["SUCCESS"],
  } as const;
  const UNDECODABLE_TERMINAL = {
    kind: "terminal",
    label: "upstream_decode_error",
    status: 502,
    errorClass: "upstream-fault",
    code: "upstream_decode_error",
    votes: ["FAILURE"],
  } as const;
  const DELIVERED_EMPTY_STREAM_REASON =
    "a head recognized as SSE is the beginning of a delivered stream, never a terminal: with the terminating frame as its only frame it ends as a valid empty stream, 200 with that frame's bytes and one SUCCESS vote, never a 502 that blames the upstream";
  const UNDECODABLE_TERMINAL_REASON =
    "a 2xx that is not SSE breaks the upstream's own claim of success: 502 upstream-fault with a FAILURE vote";
  const DELIVERED_REQ_COMPLETE_REASON =
    "a delivered stream ends with stream_done, so no req_complete line is written";
  const NON_SSE_REQ_COMPLETE_REASON =
    "req_complete names a 2xx head that is not SSE 200_NON_SSE, so an operator can tell it apart from the other ends that answer upstream_decode_error";
  const BODYLESS_REQ_COMPLETE_REASON =
    "a bodyless SSE head can never carry a frame: req_complete names it UPSTREAM_EOF_BEFORE_FIRST_FRAME, the end of a body before a first frame, never 200_NON_SSE, which names only a head that is not SSE";

  // Every row sends the same body bytes, the terminating frame; only the head
  // changes, so the head alone decides each answer, and a head recognized as
  // SSE ends as a valid empty stream, so inject resolves. A 204 or 205 cannot
  // carry the bytes: the server drops them and fetch exposes no body at all.
  it.each([
    {
      status: 200,
      head: "text/event-stream",
      contentType: "text/event-stream",
      expected: DELIVERED_EMPTY_STREAM,
      terminalReason: DELIVERED_EMPTY_STREAM_REASON,
      reqCompleteTerminals: [],
      reqCompleteReason: DELIVERED_REQ_COMPLETE_REASON,
      because: "a 2xx head declaring text/event-stream is recognized as SSE",
    },
    {
      status: 200,
      head: "text/event-stream with a charset parameter",
      contentType: "text/event-stream; charset=utf-8",
      expected: DELIVERED_EMPTY_STREAM,
      terminalReason: DELIVERED_EMPTY_STREAM_REASON,
      reqCompleteTerminals: [],
      reqCompleteReason: DELIVERED_REQ_COMPLETE_REASON,
      because:
        "the comparison is by media type, the part before the first ';', so a parameter does not change the answer",
    },
    {
      status: 200,
      head: "text/event-stream in upper case",
      contentType: "TEXT/EVENT-STREAM",
      expected: DELIVERED_EMPTY_STREAM,
      terminalReason: DELIVERED_EMPTY_STREAM_REASON,
      reqCompleteTerminals: [],
      reqCompleteReason: DELIVERED_REQ_COMPLETE_REASON,
      because: "media types compare case-insensitively",
    },
    {
      status: 200,
      head: "application/json",
      contentType: "application/json",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      reqCompleteTerminals: ["200_NON_SSE"],
      reqCompleteReason: NON_SSE_REQ_COMPLETE_REASON,
      because: "a 2xx whose media type is not text/event-stream is undecodable",
    },
    {
      status: 200,
      head: "no content-type",
      contentType: undefined,
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      reqCompleteTerminals: ["200_NON_SSE"],
      reqCompleteReason: NON_SSE_REQ_COMPLETE_REASON,
      because:
        "a 2xx without content-type makes no SSE claim, so it is not SSE (fail-closed)",
    },
    {
      status: 201,
      head: "text/event-stream and a 2xx that is not 200",
      contentType: "text/event-stream",
      expected: DELIVERED_EMPTY_STREAM,
      terminalReason: DELIVERED_EMPTY_STREAM_REASON,
      reqCompleteTerminals: [],
      reqCompleteReason: DELIVERED_REQ_COMPLETE_REASON,
      because:
        "the 2xx filter is the range 200-299, as the official SDK's response.ok: the browser EventSource's 200-only rule binds the head the gateway sends its own client, not the upstream head it consumes as a fetch client",
    },
    {
      status: 204,
      head: "text/event-stream and a status that cannot carry a body",
      contentType: "text/event-stream",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      reqCompleteTerminals: ["UPSTREAM_EOF_BEFORE_FIRST_FRAME"],
      reqCompleteReason: BODYLESS_REQ_COMPLETE_REASON,
      because:
        "a 2xx status that cannot carry a body can never carry a frame: it is the end-of-body-before-the-first-frame terminal, known at the head",
    },
    {
      status: 205,
      head: "text/event-stream and a status that cannot carry a body",
      contentType: "text/event-stream",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      reqCompleteTerminals: ["UPSTREAM_EOF_BEFORE_FIRST_FRAME"],
      reqCompleteReason: BODYLESS_REQ_COMPLETE_REASON,
      because:
        "a 2xx status that cannot carry a body can never carry a frame: it is the end-of-body-before-the-first-frame terminal, known at the head",
    },
    {
      status: 204,
      head: "application/json and a status that cannot carry a body",
      contentType: "application/json",
      expected: UNDECODABLE_TERMINAL,
      terminalReason: UNDECODABLE_TERMINAL_REASON,
      reqCompleteTerminals: ["200_NON_SSE"],
      reqCompleteReason: NON_SSE_REQ_COMPLETE_REASON,
      because:
        "the media type is read before the body: a 2xx head that is not SSE is 200_NON_SSE even when its status cannot carry a body",
    },
  ])(
    "$status with $head → $expected.label",
    async ({
      status,
      contentType,
      expected,
      because,
      terminalReason,
      reqCompleteTerminals,
      reqCompleteReason,
    }) => {
      const upstream = await holdOpenUpstream(
        status,
        contentType === undefined ? {} : { "content-type": contentType },
        "data: [DONE]\n\n",
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
        // Two questions, two assertions: the verdict on the head carries the
        // row's reason, and the answer that verdict leads to carries its own,
        // so a defect in one never fails naming the other.
        if (expected.kind === "delivered") {
          expect(res.headers["content-type"], because).toBe(
            expected.contentType,
          );
          expect(
            {
              kind: "delivered",
              label: expected.label,
              status: res.statusCode,
              contentType: res.headers["content-type"],
              body: res.body,
              votes: recorded,
            },
            terminalReason,
          ).toStrictEqual(expected);
        } else {
          const code = (res.json() as { error?: { code?: unknown } }).error
            ?.code;
          expect(code, because).toBe(expected.code);
          expect(
            {
              kind: "terminal",
              label: expected.label,
              status: res.statusCode,
              errorClass: res.headers["x-gateway-error-class"],
              code,
              votes: recorded,
            },
            terminalReason,
          ).toStrictEqual(expected);
        }

        // A third question with its own reason: which end the operator reads
        // on the request-complete line, apart from what the client was told.
        expect(
          capture.byEvent("req_complete").map((log) => log["terminal"]),
          reqCompleteReason,
        ).toStrictEqual(reqCompleteTerminals);
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

describe.each([
  { transport: "stream: true", stream: true },
  { transport: "stream: false", stream: false },
])(
  "streaming flag ON, $transport: req_complete names the pre-first-frame terminal an upstream answer decides",
  ({ stream }) => {
    // One row per terminal that an upstream answer decides before the first
    // frame, in the order and with the labels of the pre-first-frame terminals
    // table. Every row is literal: the head the upstream sends, then what the
    // gateway writes to the client, votes on the breaker and logs. The rows
    // are the same for both transports: the error path that names the
    // terminal is shared, so a request that never asked for a stream ends
    // under the same names.
    it.each<{
      terminal: string;
      upstreamStatus: number;
      upstreamBody: string;
      upstreamHeaders?: Record<string, string>;
      expected: {
        status: number;
        errorClass: string;
        body: string;
        retryAfter?: string;
        votes: ProbeOutcome[];
        events: string[];
      };
    }>([
      {
        terminal: "UPSTREAM_5XX",
        upstreamStatus: 500,
        upstreamBody: '{"error":{"type":"server_error"}}',
        expected: {
          status: 502,
          errorClass: "upstream-retry-exhausted",
          body: '{"error":{"type":"server_error"}}',
          votes: ["FAILURE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "UPSTREAM_401",
        upstreamStatus: 401,
        upstreamBody: '{"error":{"type":"server_error"}}',
        expected: {
          status: 502,
          errorClass: "upstream-auth-failure",
          body: '{"error":{"message":"upstream authentication failed","type":"server_error","code":"upstream_auth_failure"}}',
          votes: ["FAILURE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "UPSTREAM_403",
        upstreamStatus: 403,
        upstreamBody: '{"error":{"type":"server_error"}}',
        expected: {
          status: 502,
          errorClass: "upstream-access-denied",
          body: '{"error":{"message":"upstream access denied","type":"server_error","code":"upstream_access_denied"}}',
          votes: ["INCONCLUSIVE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "UPSTREAM_QUOTA",
        upstreamStatus: 429,
        upstreamBody:
          '{"error":{"type":"server_error", "code": "insufficient_quota"}}',
        upstreamHeaders: { "retry-after": "2" },
        expected: {
          status: 502,
          errorClass: "upstream-quota-exhausted",
          body: '{"error":{"message":"upstream quota exhausted","type":"server_error","code":"upstream_quota_exhausted"}}',
          votes: ["INCONCLUSIVE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "UPSTREAM_429",
        upstreamStatus: 429,
        upstreamBody: '{"error":{"type":"server_error"}}',
        upstreamHeaders: { "retry-after": "2" },
        expected: {
          status: 429,
          errorClass: "upstream-retry-exhausted",
          body: '{"error":{"type":"server_error"}}',
          retryAfter: "2",
          votes: ["INCONCLUSIVE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "UPSTREAM_OTHER_4XX",
        upstreamStatus: 402,
        upstreamBody: '{"error":{"type":"server_error"}}',
        expected: {
          status: 402,
          errorClass: "client-fault",
          body: '{"error":{"type":"server_error"}}',
          votes: ["INCONCLUSIVE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "REDIRECT_BLOCKED",
        upstreamStatus: 301,
        upstreamBody: '{"error":{"type":"server_error"}}',
        expected: {
          status: 502,
          errorClass: "upstream-redirect-blocked",
          body: '{"error":{"message":"upstream redirect blocked","type":"server_error","code":"upstream_redirect_blocked"}}',
          votes: ["FAILURE"],
          events: ["req_start", "req_complete"],
        },
      },
      {
        terminal: "RESPONSE_TOO_LARGE",
        upstreamStatus: 502,
        upstreamBody: "x".repeat(2 * 1024 * 1024),
        expected: {
          status: 502,
          errorClass: "upstream-fault",
          body: '{"error":{"message":"upstream response too large","type":"server_error","code":"response_too_large"}}',
          votes: ["INCONCLUSIVE"],
          events: ["req_start", "req_complete"],
        },
      },
    ])(
      "ends as $terminal when the upstream answers $upstreamStatus",
      async ({
        terminal,
        upstreamStatus,
        upstreamBody,
        upstreamHeaders,
        expected,
      }) => {
        const upstream = await answeringUpstream(
          upstreamStatus,
          upstreamBody,
          upstreamHeaders,
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
              retryAfter: res.headers["retry-after"],
              votes: recorded,
            },
            `${terminal}: what the gateway writes to the client and votes on the breaker is this terminal's row of the pre-first-frame table; an upstream Retry-After is echoed only where the upstream's own answer passes through`,
          ).toStrictEqual({
            status: expected.status,
            errorClass: expected.errorClass,
            body: expected.body,
            retryAfter: expected.retryAfter,
            votes: expected.votes,
          });
          // The operator alert is emitted once per process for each alert name,
          // so whether it appears here depends on which requests ran before
          // this one. Its own tests pin it; this list holds the events of the
          // request itself.
          expect(
            capture.logs
              .flatMap((log) => log.event ?? [])
              .filter((event) => event !== "operational_alert"),
            `${terminal}: a pre-first-frame terminal emits req_complete, never stream_done`,
          ).toStrictEqual(expected.events);
          expect(
            capture.byEvent("req_complete")[0],
            `req_complete names the terminal ${terminal}, so an operator can tell which pre-first-frame row ended the request`,
          ).toMatchObject({
            status: expected.status,
            error_class: expected.errorClass,
            stream,
            terminal,
          });
        } finally {
          await app.close();
          await upstream.close();
        }
      },
    );
  },
);

// A loopback port with nothing listening: every connection attempt is refused
// before a request byte leaves the socket.
async function refusedPort(): Promise<number> {
  const server = await listenEphemeral(http.createServer());
  await server.close();
  return server.port;
}

describe.each([
  { transport: "stream: true", stream: true },
  { transport: "stream: false", stream: false },
])(
  "streaming flag ON, $transport: req_complete names the pre-first-frame terminals no upstream answer decides",
  ({ stream }) => {
    it("ends as RETRY_EXHAUSTED when every attempt is refused before a request byte is sent", async () => {
      const port = await refusedPort();
      const client = createOpenAIClient({
        apiKey: "gateway-key",
        baseURL: `http://127.0.0.1:${port}`,
      });
      const capture = makeLogCapture();
      const violations: string[] = [];
      const { breaker, recorded } = recordingBreaker();
      const app = await buildProxyApp({
        breaker,
        logger: capture.logger,
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
            body: res.json(),
            votes: recorded,
          },
          "a network failure proven before the request was sent, on every attempt of the retry budget, is a 502 upstream-retry-exhausted with one FAILURE vote",
        ).toStrictEqual({
          status: 502,
          errorClass: "upstream-retry-exhausted",
          body: {
            error: {
              message: "upstream unavailable",
              type: "server_error",
              code: "upstream_unavailable",
            },
          },
          votes: ["FAILURE"],
        });
        expect(
          capture.byEvent("req_complete"),
          "req_complete names the terminal, so an operator can tell an exhausted pre-send retry from a network failure after the request was sent",
        ).toMatchObject([
          {
            status: 502,
            error_class: "upstream-retry-exhausted",
            stream,
            attempts: 2,
            retry_disposition: "attempted",
            terminal: "RETRY_EXHAUSTED",
          },
        ]);
      } finally {
        await app.close();
      }
    });

    it("ends as FAST_FAIL when the breaker refuses the request, before any attempt", async () => {
      const capture = makeLogCapture();
      const violations: string[] = [];
      const recorded: ProbeOutcome[] = [];
      const buffered = bufferedTripwire(violations);
      const streaming = streamingTripwire(violations);
      const app = await buildProxyApp({
        breaker: {
          tryAcquire: () => ({ kind: "FAST_FAIL" }),
          recordResult: (outcome) => {
            recorded.push(outcome);
          },
          getState: () => "OPEN",
        },
        logger: capture.logger,
        streamingEnabled: true,
        upstreamBuffered: buffered.seam,
        upstreamStreaming: streaming.seam,
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
            body: res.json(),
            upstreamCalls: buffered.calls() + streaming.calls(),
            votes: recorded,
          },
          "a breaker that refuses the request answers 503 circuit_breaker_open without calling the upstream, and the refusal is not a vote",
        ).toStrictEqual({
          status: 503,
          errorClass: "upstream-retry-exhausted",
          body: {
            error: {
              message: "Service temporarily unavailable",
              type: "service_unavailable",
              code: "circuit_breaker_open",
            },
          },
          upstreamCalls: 0,
          votes: [],
        });
        expect(
          capture.byEvent("req_complete"),
          "req_complete names the terminal, so an operator can tell a breaker refusal from an exhausted retry, which shares its error class",
        ).toMatchObject([
          {
            status: 503,
            error_class: "upstream-retry-exhausted",
            stream,
            attempts: 0,
            retry_disposition: "ineligible",
            terminal: "FAST_FAIL",
          },
        ]);
      } finally {
        await app.close();
      }
    });
  },
);

// A gate whose every slot is taken: the gate's own capacity rules are pinned
// by its tests; these cells pin what the request does when it is refused.
const FULL_GATE = { tryAcquire: () => null };

describe.each([
  { transport: "stream: true", stream: true },
  { transport: "stream: false", stream: false },
])(
  "streaming flag ON, $transport: an admission refusal ends before the request lifecycle",
  ({ stream }) => {
    it.each([
      {
        door: "before authentication",
        gates: { admissionGatePre: FULL_GATE },
      },
      {
        door: "after authentication",
        gates: { admissionGatePost: FULL_GATE },
      },
    ])(
      "answers 503 admission_full $door with no event, no vote and no upstream call",
      async ({ gates }) => {
        const capture = makeLogCapture();
        const violations: string[] = [];
        const { breaker, recorded } = recordingBreaker();
        const buffered = bufferedTripwire(violations);
        const streaming = streamingTripwire(violations);
        const app = await buildProxyApp({
          breaker,
          logger: capture.logger,
          streamingEnabled: true,
          upstreamBuffered: buffered.seam,
          upstreamStreaming: streaming.seam,
          ...gates,
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
              body: res.json(),
              upstreamCalls: buffered.calls() + streaming.calls(),
              votes: recorded,
            },
            "a full admission gate answers 503 admission_full in the service_unavailable lane, with no error class, before the breaker or the upstream is asked",
          ).toStrictEqual({
            status: 503,
            errorClass: undefined,
            body: {
              error: {
                message: "Service temporarily unavailable",
                type: "service_unavailable",
                code: "admission_full",
              },
            },
            upstreamCalls: 0,
            votes: [],
          });
          expect(
            capture.logs.flatMap((log) => log.event ?? []),
            "an admission refusal happens before the handler: it emits no event at all, not even req_start",
          ).toStrictEqual([]);
        } finally {
          await app.close();
        }
      },
    );
  },
);

describe("streaming flag ON, stream: false: req_complete names a network failure after the request was sent", () => {
  it("ends as NETWORK_FAILED_POST_SEND when the connection is destroyed after the head, during the buffered body read", async () => {
    const upstream = await resettingUpstream(200, {
      "content-type": "application/json",
    });
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
      upstreamBuffered: bufferedTripwire(violations, client.buffered).seam,
      upstreamStreaming: streamingTripwire(violations).seam,
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: false },
      });

      expect(violations).toStrictEqual([]);
      expect(
        {
          status: res.statusCode,
          errorClass: res.headers["x-gateway-error-class"],
          body: res.json(),
          votes: recorded,
        },
        "a connection that dies after the head, while a stream: false body is read, is a network failure after the request was sent: 504 gateway-fault with exactly one FAILURE vote and no retry",
      ).toStrictEqual({
        status: 504,
        errorClass: "gateway-fault",
        body: {
          error: {
            message: "gateway error",
            type: "gateway_error",
            code: "upstream_connection_failed",
          },
        },
        votes: ["FAILURE"],
      });
      expect(
        capture.byEvent("req_complete"),
        "the stream: false request ends under the same name as the stream: true one, because the error path that names it is shared",
      ).toMatchObject([
        {
          status: 504,
          error_class: "gateway-fault",
          stream: false,
          attempts: 1,
          retry_disposition: "ineligible",
          terminal: "NETWORK_FAILED_POST_SEND",
        },
      ]);
    } finally {
      await app.close();
      await upstream.close();
    }
  });
});

describe("streaming flag ON, stream: true: the cost caps of the buffered body hold", () => {
  // The output-token ceiling, literal: a stream: true body is bound by the same
  // number as a buffered one.
  const OUTPUT_TOKEN_CEILING = 16_384;

  it.each([
    {
      field: "max_tokens above the ceiling",
      extra: { max_tokens: OUTPUT_TOKEN_CEILING + 1 },
      code: "max_tokens_too_large",
    },
    {
      field: "max_completion_tokens above the ceiling",
      extra: { max_completion_tokens: OUTPUT_TOKEN_CEILING + 1 },
      code: "max_completion_tokens_too_large",
    },
    {
      field: "a choice count above one",
      extra: { n: 2 },
      code: "n_not_supported",
    },
  ])(
    "rejects $field with the buffered body's 400, before the breaker and the upstream",
    async ({ extra, code }) => {
      const capture = makeLogCapture();
      const violations: string[] = [];
      const { breaker, recorded } = recordingBreaker();
      const buffered = bufferedTripwire(violations);
      const streaming = streamingTripwire(violations);
      const app = await buildProxyApp({
        breaker,
        logger: capture.logger,
        streamingEnabled: true,
        upstreamBuffered: buffered.seam,
        upstreamStreaming: streaming.seam,
      });

      try {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: true, ...extra },
        });

        expect(violations).toStrictEqual([]);
        expect(
          {
            status: res.statusCode,
            errorClass: res.headers["x-gateway-error-class"],
            code: (res.json() as { error?: { code?: unknown } }).error?.code,
            upstreamCalls: buffered.calls() + streaming.calls(),
            votes: recorded,
          },
          "a stream: true body that breaks a cost cap gets the same 400 and code as the identical buffered body, never reaches the upstream and never votes",
        ).toStrictEqual({
          status: 400,
          errorClass: "client-fault",
          code,
          upstreamCalls: 0,
          votes: [],
        });
        expect(
          capture.logs.flatMap((log) => log.event ?? []),
          "a cost-cap rejection logs one req_rejected and no request lifecycle: no req_start, no req_complete",
        ).toStrictEqual(["req_rejected"]);
        expect(
          capture.byEvent("req_rejected")[0],
          "a cost-cap rejection is told apart from a shape error by its reason",
        ).toMatchObject({ reason: "cost_cap_exceeded" });
      } finally {
        await app.close();
      }
    },
  );

  it.each([
    {
      sent: "no output-token field",
      extra: {},
      forwarded: { max_tokens: OUTPUT_TOKEN_CEILING, max_completion_tokens: undefined },
      because:
        "a stream: true body that omits both output-token fields is forwarded with the ceiling injected, so omission never reaches the upstream uncapped",
    },
    {
      sent: "max_tokens under the ceiling",
      extra: { max_tokens: 100 },
      forwarded: { max_tokens: 100, max_completion_tokens: undefined },
      because: "a client's own max_tokens under the ceiling is forwarded as sent, never replaced",
    },
    {
      sent: "only max_completion_tokens",
      extra: { max_completion_tokens: 100 },
      forwarded: { max_tokens: undefined, max_completion_tokens: 100 },
      because:
        "a client's own max_completion_tokens already bounds the output, so no max_tokens is injected beside it",
    },
  ])(
    "forwards a stream: true body with $sent to the streaming upstream with the buffered body's output bound",
    async ({ extra, forwarded, because }) => {
      const violations: string[] = [];
      const received: Record<string, unknown>[] = [];
      const { breaker } = recordingBreaker();
      const app = await buildProxyApp({
        breaker,
        streamingEnabled: true,
        upstreamBuffered: bufferedTripwire(violations).seam,
        upstreamStreaming: (body) => {
          received.push({ ...(body as Record<string, unknown>) });
          return Promise.resolve({
            kind: "undecodable",
            undecodable_kind: "non_sse_head",
          });
        },
      });

      try {
        await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: true, ...extra },
        });

        expect(violations).toStrictEqual([]);
        expect(
          received.map((body) => ({
            stream: body["stream"],
            max_tokens: body["max_tokens"],
            max_completion_tokens: body["max_completion_tokens"],
          })),
          because,
        ).toStrictEqual([{ stream: true, ...forwarded }]);
      } finally {
        await app.close();
      }
    },
  );
});

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
  const UNHANDLED_EXCEPTION_BODY = {
    error: {
      message: "internal server error",
      type: "internal_error",
      code: "unhandled_exception",
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

  // A fake accepted stream whose first read rejects with an error the
  // recognizer does not know: not the signal's reason, no cause code, no
  // cause message.
  function unrecognizedRejectingStream(): AcceptedStream {
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.error(new Error("a rejection the recognizer does not know"));
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

  it("ends a first-frame read that rejects with an error the recognizer does not know as UNRECOGNIZED_REJECTION: one INCONCLUSIVE vote, the body cancelled, the error handler's 500", async () => {
    const capture = makeLogCapture();
    const violations: string[] = [];
    const { breaker, recorded } = recordingBreaker();

    const app = await buildProxyApp({
      breaker,
      logger: capture.logger,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: () => Promise.resolve(unrecognizedRejectingStream()),
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
        "an unrecognized rejection of the first-frame read is rethrown to the scoped error handler: the client gets its 500",
      ).toBe(500);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(
        res.json(),
        "an unrecognized rejection of the first-frame read gets the error handler's generic body, with nothing of the error in it",
      ).toStrictEqual(UNHANDLED_EXCEPTION_BODY);
      expect(
        recorded,
        "an unrecognized rejection proves nothing about the upstream: exactly one INCONCLUSIVE breaker result, recorded before the rethrow",
      ).toStrictEqual(["INCONCLUSIVE"]);

      expect(
        capture.logs.flatMap((log) => log.event ?? []),
        "the rejection already errored the body, so the route's cancel on every exit is seen as the cancel-rejected observation; req_complete is emitted once, before the rethrow, and no stream_done",
      ).toStrictEqual(["req_start", "upstream_cancel_rejected", "req_complete"]);
      expect(
        capture.byEvent("upstream_cancel_rejected")[0],
        "the route, not the adapter, cancels the body it received still open",
      ).toMatchObject({ site: "proxy_route" });
      expect(
        capture.byEvent("req_complete")[0],
        "req_complete names the terminal, so an operator can tell an unrecognized rejection from any other 500",
      ).toMatchObject({
        status: 500,
        error_class: "gateway-fault",
        stream: true,
        attempts: 1,
        terminal: "UNRECOGNIZED_REJECTION",
      });
    } finally {
      await app.close();
    }
  });
});

// A fake accepted stream the cell drives by hand. `release(text)` makes the
// pending read resolve with those bytes, `end()` closes the body and
// `fail(error)` errors it; an abort of the request signal
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

function inject(app: Awaited<ReturnType<typeof buildProxyApp>>) {
  return app.inject({
    method: "POST",
    url: "/v1/chat/completions",
    headers: { authorization: bearer() },
    payload: { ...validBody, stream: true },
  });
}

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

  it("lets a settled FIRST_FRAME_NOT_JSON stand when the deadline is reached while its cleanup is still awaited: the firing does nothing", async () => {
    const t = await buildDrivenApp(true);
    vi.useFakeTimers();

    try {
      const req = inject(t.app);
      await vi.advanceTimersByTimeAsync(1_000);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.calls(),
        "the scene's premise: the streaming upstream was called once and answered with an accepted head",
      ).toBe(1);

      t.driven().release(NOT_JSON_FRAME);
      await vi.advanceTimersByTimeAsync(0);
      expect(
        t.driven().cancelCalled(),
        "the scene's premise: the terminal is settled and its cleanup, the body's cancel, is pending",
      ).toBe(true);
      expect(
        t.recorded,
        "the scene's premise: the settled terminal recorded its single breaker result before the cleanup",
      ).toStrictEqual(["FAILURE"]);

      await vi.advanceTimersByTimeAsync(280_000);
      expect(
        t.signal()?.aborted,
        "a firing that finds a terminal already settled does nothing: the request signal is not aborted",
      ).toBe(false);

      t.driven().finishCancel();
      await vi.advanceTimersByTimeAsync(0);

      const res = await req;
      expect(
        res.statusCode,
        "the settled terminal stands: the client gets the FIRST_FRAME_NOT_JSON 502, not a 504",
      ).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe("upstream-fault");
      expect(
        t.recorded,
        "the settled terminal's single breaker result stands: no second result from the firing",
      ).toStrictEqual(["FAILURE"]);
      expect(
        t.capture.logs.flatMap((log) => log.event ?? []),
        "one terminal, one req_complete, and no stream_done",
      ).toStrictEqual(["req_start", "req_complete"]);
      expect(t.capture.byEvent("req_complete")[0]).toMatchObject({
        status: 502,
        error_class: "upstream-fault",
        stream: true,
        attempts: 1,
        terminal: "FIRST_FRAME_NOT_JSON",
      });
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });

  it("commits the head and a JSON frame available 1 ms before the deadline together: a 200 SSE head, exactly that frame's bytes, one stream_first_token, and nothing after the line", async () => {
    const t = await buildDrivenApp();
    // A real client socket, not inject: on this branch the committed response
    // stays open after the line (the forwarding loop is the next slice), so
    // the client reads the head plus the first body bytes and then closes.
    await t.app.listen({ port: 0, host: "127.0.0.1" });
    const address = t.app.server.address();
    if (address === null || typeof address === "string") {
      throw new Error("the app did not listen on a TCP port");
    }
    // setImmediate stays real so the test can yield to socket I/O while the
    // clock is fake; no timer is advanced in this scene, only the clock moves.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const startedAt = Date.now();
    const yieldToIo = () =>
      new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    const until = async (ready: () => boolean, premise: string) => {
      for (let turn = 0; turn < 200; turn += 1) {
        if (ready()) return;
        await yieldToIo();
      }
      throw new Error(premise);
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
    let head: http.IncomingMessage | undefined;
    const received: Buffer[] = [];
    client.on("response", (res) => {
      head = res;
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
      expect(t.violations).toStrictEqual([]);
      expect(
        t.signal()?.aborted,
        "the scene's premise: the deadline's timer has not fired, so the request signal is not aborted",
      ).toBe(false);

      // The frame becomes available with the clock 1 ms short of the deadline
      // value: the pair of row 1 (clock exactly at the deadline), which is what
      // tells `>=` from `>`.
      vi.setSystemTime(startedAt + 279_999);
      t.driven().release(JSON_FRAME);

      await until(
        () => head !== undefined,
        "the client never received a response head after the frame was released",
      );
      expect(
        head?.statusCode,
        "a JSON first frame available 1 ms before the deadline crosses the first-frame line: the client sees a 200, never a 5xx terminal",
      ).toBe(200);
      expect(
        head?.headers["content-type"],
        "the commit writes the SSE head: content-type exactly text/event-stream",
      ).toBe("text/event-stream");

      const frameBytes = Buffer.from(JSON_FRAME, "utf8");
      await until(
        () => Buffer.concat(received).length >= frameBytes.length,
        "the client never received the first frame's bytes after the head",
      );
      expect(
        Buffer.concat(received).equals(frameBytes),
        "the body the client has read is exactly the first frame's bytes, forwarded verbatim: no error body, no terminating frame",
      ).toBe(true);

      // The client leaves right after the frame. Nothing on this branch ends
      // the response or cancels the upstream body after the line: the
      // departure observer (6.11) and the post-loop (6.12) are later slices,
      // and the fake body is closed by this test's teardown.
      client.destroy();
      await yieldToIo();

      expect(
        t.capture.logs.flatMap((log) => log.event ?? []),
        "after the commit the request has logged req_start and stream_first_token, in that order, and nothing else: no req_complete, no stream_done, no alert, no cancel-rejected line",
      ).toStrictEqual(["req_start", "stream_first_token"]);
      expect(
        t.capture.byEvent("stream_first_token")[0],
        "ttft_ms is measured from the attempt's dispatch to the first complete parsed frame: with the clock moved to 1 ms short of the 280 000 ms deadline, 279 999; was_cold_start is the value req_start snapshotted",
      ).toMatchObject({
        ttft_ms: 279_999,
        was_cold_start: t.capture.byEvent("req_start")[0]?.was_cold_start,
      });
      expect(
        t.recorded,
        "the breaker votes once, at the end of the stream, and the end is not reached on this slice: no breaker result at the line",
      ).toStrictEqual([]);
    } finally {
      client.destroy();
      vi.useRealTimers();
      await t.app.close();
    }
  });
});

describe("streaming flag ON: a first data frame that is the terminating frame is a valid, empty stream", () => {
  it("commits the head and the terminating frame together, ends as DONE with one SUCCESS vote and no first-token line, and cancels the upstream body", async () => {
    const upstream = await holdOpenUpstream(
      200,
      { "content-type": "text/event-stream" },
      "data: [DONE]\n\n",
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
        "the terminating frame as the first data frame is a valid, empty stream: the gateway commits with a 200, never a decode error",
      ).toBe(200);
      expect(
        res.headers["content-type"],
        "the commit writes the SSE head: content-type text/event-stream",
      ).toBe("text/event-stream");
      expect(
        res.body,
        "the body is exactly the terminating frame's bytes, forwarded verbatim, with nothing before or after it",
      ).toBe("data: [DONE]\n\n");
      expect(
        recorded,
        "a stream that ends with the upstream's own terminating frame is a clean completion: exactly one SUCCESS breaker result",
      ).toStrictEqual(["SUCCESS"]);

      const finished = await withinDeadline(
        upstream.closed,
        UPSTREAM_CLOSE_DEADLINE_MS,
        "the post-loop did not cancel the upstream body after DONE: the fake saw no close before the deadline",
      );
      expect(
        finished,
        "the upstream connection closed only after the fake ended the body, not by a cancel",
      ).toBe(false);

      expect(
        capture.logs.flatMap((log) => log.event ?? []),
        "after the commit the terminal event is stream_done, never req_complete; and a stream whose only data frame is the terminating frame never fires stream_first_token, because that measure promises time to content",
      ).toStrictEqual(["req_start", "stream_done"]);
      expect(
        capture.byEvent("stream_done")[0],
        "stream_done carries the clean completion: completed true, terminal DONE, no error class",
      ).toMatchObject({
        completed: true,
        terminal: "DONE",
        error_class: null,
        attempts: 1,
      });
    } finally {
      await app.close();
      await upstream.close();
    }
  });
});

describe("streaming flag ON: the terminating-frame-first path ends without waiting for the upstream cancel", () => {
  // Polling interval while the cell waits for a log line: short enough that
  // the wait costs nothing against the deadline, long enough not to spin.
  const LOG_POLL_MS = 5;

  // A fake accepted stream whose only bytes are the terminating frame and
  // whose cancel() never settles: any step of the route that waits for the
  // cancel waits forever, so the cell sees that wait as a missing event.
  function doneFirstStreamWithStuckCancel(): {
    stream: AcceptedStream;
    cancelCalled: () => boolean;
  } {
    let cancelCalled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
      },
      pull() {
        return new Promise<void>(() => {});
      },
      cancel() {
        cancelCalled = true;
        return new Promise<void>(() => {});
      },
    });

    return {
      stream: { kind: "accepted_stream", status: 200, reader: body.getReader() },
      cancelCalled: () => cancelCalled,
    };
  }

  // A one-slot POST_AUTH gate that reports whether its slot came back.
  function observedPostGate(): {
    gate: AdmissionGate;
    released: () => boolean;
  } {
    const inner = createAdmissionGate(1);
    let released = false;

    return {
      gate: {
        tryAcquire() {
          const release = inner.tryAcquire();
          if (release === null) {
            return null;
          }
          return () => {
            release();
            released = true;
          };
        },
      },
      released: () => released,
    };
  }

  async function untilLogged(
    capture: LogCapture,
    event: string,
    stopped: () => boolean,
  ): Promise<void> {
    while (capture.byEvent(event).length === 0 && !stopped()) {
      await new Promise((resolve) => setTimeout(resolve, LOG_POLL_MS));
    }
  }

  it("emits one DONE stream_done, votes SUCCESS once and releases the admission slot while the body's cancel never settles", async () => {
    const capture = makeLogCapture();
    const violations: string[] = [];
    const { breaker, recorded } = recordingBreaker();
    const post = observedPostGate();
    let fake: ReturnType<typeof doneFirstStreamWithStuckCancel> | undefined;
    let waiting = true;
    const app = await buildProxyApp({
      breaker,
      logger: capture.logger,
      streamingEnabled: true,
      admissionGatePost: post.gate,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: () => {
        fake = doneFirstStreamWithStuckCancel();
        return Promise.resolve(fake.stream);
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
        res.statusCode,
        "the terminating frame as the first data frame commits a 200 stream",
      ).toBe(200);
      expect(
        res.headers["content-type"],
        "the commit writes the SSE head: content-type text/event-stream",
      ).toBe("text/event-stream");
      expect(
        res.body,
        "the response carries the upstream's original terminating bytes and ends, whether or not the cancel settles",
      ).toBe("data: [DONE]\n\n");

      await withinDeadline(
        untilLogged(capture, "stream_done", () => !waiting),
        UPSTREAM_CLOSE_DEADLINE_MS,
        "the terminal event does not wait for the upstream cancel: with the body's cancel() never settling, no stream_done was emitted before the deadline",
      );

      expect(
        fake?.cancelCalled(),
        "the post-loop starts the body reader's cancel even after DONE",
      ).toBe(true);
      expect(
        capture.byEvent("stream_done"),
        "exactly one terminal event, emitted without the cancel settling",
      ).toHaveLength(1);
      expect(
        capture.byEvent("stream_done")[0],
        "the terminal event is the clean completion: completed true, terminal DONE",
      ).toMatchObject({ completed: true, terminal: "DONE" });
      expect(
        recorded,
        "the breaker result is recorded exactly once, as SUCCESS, without the cancel settling",
      ).toStrictEqual(["SUCCESS"]);
      expect(
        post.released(),
        "the admission slot is released without the cancel settling",
      ).toBe(true);
    } finally {
      waiting = false;
      await app.close();
    }
  });
});

// After the first frame the loop forwards every frame's original bytes until a
// terminal. The JSON payloads below carry spaces on purpose: a gateway that
// parsed and re-serialized a frame would drop them, so byte equality is what
// proves the forwarding is verbatim.
const CONTENT_FRAMES =
  'data: {"id": "c1", "choices": [{"delta": {"content": "Hel"}}]}\n\n' +
  'data: {"id": "c1", "choices": [{"delta": {"content": "lo"}}]}\n\n' +
  'data: {"id": "c1", "choices": [{"delta": {}, "finish_reason": "stop"}]}\n\n';
const ONE_CONTENT_FRAME =
  'data: {"id": "c1", "choices": [{"delta": {"content": "Hel"}}]}\n\n';

// The same loopback measurement as RESET_AFTER_HEAD_MS: 50 ms lets the bytes
// already written reach the gateway before the connection dies (inferred for
// body bytes from the head measurement of 2026-10-07).
const RESET_AFTER_FRAMES_MS = RESET_AFTER_HEAD_MS;

// A fake upstream that sends an SSE head and the given frames, then destroys
// the connection without ending the body: after the first frame, the gateway's
// next read rejects with a transport error.
async function framesThenResetUpstream(frames: string): Promise<FakeUpstream> {
  const server = http.createServer((req, res) => {
    res.on("error", () => {});
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write(frames);
      setTimeout(() => res.destroy(), RESET_AFTER_FRAMES_MS);
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

async function streamThrough(upstream: FakeUpstream) {
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

  // A response that never ends leaves inject pending: the deadline turns that
  // absence into a RED that names the missing end.
  const res = await withinDeadline(
    app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer() },
      payload: { ...validBody, stream: true },
    }),
    UPSTREAM_CLOSE_DEADLINE_MS,
    "the stream never ended: after the first frame the loop must read every frame until a terminal and end the response",
  );

  return { app, res, capture, violations, recorded };
}

describe("streaming flag ON: the loop forwards every frame verbatim and ends at the terminating frame", () => {
  it("answers 200 SSE whose body is the upstream's bytes, with one first-token line, one DONE stream_done and one SUCCESS vote", async () => {
    const upstream = await endingUpstream(
      200,
      { "content-type": "text/event-stream" },
      CONTENT_FRAMES + "data: [DONE]\n\n",
    );
    const t = await streamThrough(upstream);

    try {
      expect(t.violations).toStrictEqual([]);
      expect(
        t.res.statusCode,
        "a stream that reaches the upstream's terminating frame keeps the 200 of its commit",
      ).toBe(200);
      expect(
        t.res.headers["content-type"],
        "the commit writes the SSE head: content-type exactly text/event-stream",
      ).toBe("text/event-stream");
      expect(
        t.res.rawPayload,
        "every frame is forwarded byte-for-byte, the terminating frame included: the gateway never re-serializes a frame",
      ).toStrictEqual(Buffer.from(CONTENT_FRAMES + "data: [DONE]\n\n", "utf8"));
      expect(
        t.capture.logs.flatMap((log) => log.event ?? []),
        "a delivered stream logs req_start, one stream_first_token at the first frame and one stream_done at its end, and nothing else",
      ).toStrictEqual(["req_start", "stream_first_token", "stream_done"]);
      expect(
        t.capture.byEvent("stream_done")[0],
        "the terminal event is the clean completion: completed true, terminal DONE, no error class, one attempt",
      ).toMatchObject({
        completed: true,
        terminal: "DONE",
        error_class: null,
        attempts: 1,
      });
      expect(
        t.recorded,
        "the breaker votes once, at the end of the stream: SUCCESS, the only vote a clean completion casts",
      ).toStrictEqual(["SUCCESS"]);
    } finally {
      await t.app.close();
      await upstream.close();
    }
  });
});

describe("streaming flag ON: an upstream that stops without its terminating frame ends with one sentinel", () => {
  it.each([
    {
      terminal: "UPSTREAM_DEATH",
      scene: "the connection resets after a forwarded frame",
      start: () => framesThenResetUpstream(ONE_CONTENT_FRAME),
      sentinel:
        'data: {"error": {"message": "stream interrupted", "type": "upstream_error", "code": "stream_aborted"}, "gateway": {"terminal": "UPSTREAM_DEATH"}}\n\n',
      payload: {
        error: {
          message: "stream interrupted",
          type: "upstream_error",
          code: "stream_aborted",
        },
        gateway: { terminal: "UPSTREAM_DEATH" },
      },
      errorClass: "gateway-fault",
      rule: "a transport error after the first frame is UPSTREAM_DEATH, classed by the shared classifier: a reset after the send carries no proof of non-execution, so gateway-fault",
    },
    {
      terminal: "EOF_WITHOUT_DONE",
      scene: "the body ends cleanly after a forwarded frame, with no terminating frame",
      start: () =>
        endingUpstream(
          200,
          { "content-type": "text/event-stream" },
          ONE_CONTENT_FRAME,
        ),
      sentinel:
        'data: {"error": {"message": "stream ended without completion", "type": "upstream_error", "code": "stream_truncated"}, "gateway": {"terminal": "EOF_WITHOUT_DONE"}}\n\n',
      payload: {
        error: {
          message: "stream ended without completion",
          type: "upstream_error",
          code: "stream_truncated",
        },
        gateway: { terminal: "EOF_WITHOUT_DONE" },
      },
      errorClass: "upstream-fault",
      rule: "an EOF is never the terminating frame: a clean close without [DONE] is EOF_WITHOUT_DONE, upstream-fault, the same class as the end of a body before the first frame",
    },
    {
      terminal: "EOF_WITHOUT_DONE",
      scene: "the body ends in the middle of a frame, after a forwarded frame",
      start: () =>
        endingUpstream(
          200,
          { "content-type": "text/event-stream" },
          ONE_CONTENT_FRAME + 'data: {"id": "c1", "choices": [{"delta": {"content": "w',
        ),
      sentinel:
        'data: {"error": {"message": "stream ended without completion", "type": "upstream_error", "code": "stream_truncated"}, "gateway": {"terminal": "EOF_WITHOUT_DONE"}}\n\n',
      payload: {
        error: {
          message: "stream ended without completion",
          type: "upstream_error",
          code: "stream_truncated",
        },
        gateway: { terminal: "EOF_WITHOUT_DONE" },
      },
      errorClass: "upstream-fault",
      rule: "a close in the middle of a frame is EOF_WITHOUT_DONE too, upstream-fault: the partial bytes never formed a frame, so they are never forwarded",
    },
    {
      terminal: "PARSER_BUFFER_CAP",
      scene: "bytes pass the frame buffer cap without forming a frame, after a forwarded frame",
      start: () =>
        holdOpenUpstream(
          200,
          { "content-type": "text/event-stream" },
          ONE_CONTENT_FRAME + `data: ${"x".repeat(PARSER_BUFFER_CAP + 1)}`,
        ),
      sentinel:
        'data: {"error": {"message": "stream frame could not be decoded", "type": "upstream_error", "code": "upstream_decode_error"}, "gateway": {"terminal": "PARSER_BUFFER_CAP"}}\n\n',
      payload: {
        error: {
          message: "stream frame could not be decoded",
          type: "upstream_error",
          code: "upstream_decode_error",
        },
        gateway: { terminal: "PARSER_BUFFER_CAP" },
      },
      errorClass: "upstream-fault",
      rule: "bytes that pass the frame buffer cap after the first frame end as PARSER_BUFFER_CAP, upstream-fault, the same class as the cap before the first frame; none of the buffered bytes are forwarded",
    },
  ])(
    "$terminal: $scene",
    async ({ terminal, start, sentinel, payload, errorClass, rule }) => {
      const upstream = await start();
      const t = await streamThrough(upstream);

      try {
        expect(t.violations).toStrictEqual([]);
        expect(
          t.res.statusCode,
          `${terminal}: an abnormal end after the commit keeps the 200; the status never changes retroactively`,
        ).toBe(200);

        // The terminal event first: a gateway that picked the wrong terminal
        // fails here, naming the row's rule, before the body checks that
        // would only name the missing sentinel.
        expect(
          t.capture.byEvent("stream_done"),
          `${terminal}: exactly one terminal event`,
        ).toHaveLength(1);
        expect(t.capture.byEvent("stream_done")[0], rule).toMatchObject({
          completed: false,
          terminal,
          error_class: errorClass,
        });

        expect(
          t.res.body.startsWith(ONE_CONTENT_FRAME),
          `${terminal}: the forwarded frame comes first, unaltered`,
        ).toBe(true);

        const rest = t.res.body.slice(ONE_CONTENT_FRAME.length);
        expect(
          rest.startsWith("data: ") && rest.endsWith("\n\n"),
          `${terminal}: after the forwarded frame the body holds exactly one gateway frame, the sentinel`,
        ).toBe(true);

        // A payload that is not JSON must fail the assertion below with its
        // sentence, not throw a bare SyntaxError that names no rule.
        let sentinelPayload: unknown;
        try {
          sentinelPayload = JSON.parse(
            rest.slice("data: ".length, -"\n\n".length),
          );
        } catch {
          sentinelPayload = rest;
        }
        expect(
          sentinelPayload,
          `${terminal}: the sentinel carries that terminal's row of the sentinel vocabulary and names the terminal in its gateway object`,
        ).toStrictEqual(payload);
        expect(
          t.res.rawPayload,
          `${terminal}: the body is the forwarded frame followed by the sentinel in the byte form of the spec's wire example, and nothing else`,
        ).toStrictEqual(Buffer.from(ONE_CONTENT_FRAME + sentinel, "utf8"));

        expect(
          t.recorded,
          `${terminal}: every terminal after the first frame other than DONE votes INCONCLUSIVE, exactly once`,
        ).toStrictEqual(["INCONCLUSIVE"]);
      } finally {
        await t.app.close();
        await upstream.close();
      }
    },
  );
});

describe("streaming flag ON: after the first-frame line, the total-duration deadline ends the stream inside it", () => {
  const TOTAL_TIMEOUT_SENTINEL =
    'data: {"error": {"message": "stream exceeded the total duration limit", "type": "gateway_timeout", "code": "total_timeout_exceeded"}, "gateway": {"terminal": "TOTAL_TIMEOUT"}}\n\n';

  it("ends a committed stream as TOTAL_TIMEOUT when the deadline's timer fires on time with the loop parked in the frame read", async () => {
    const t = await buildDrivenApp();
    vi.useFakeTimers();

    try {
      const req = inject(t.app);
      await vi.advanceTimersByTimeAsync(1_000);
      t.driven().release(ONE_CONTENT_FRAME);
      await vi.advanceTimersByTimeAsync(0);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.capture.byEvent("stream_first_token"),
        "the scene's premise: the first frame crossed the first-frame line, so the status is sealed at 200",
      ).toHaveLength(1);
      expect(
        t.signal()?.aborted,
        "the scene's premise: the deadline's timer has not fired, so the loop is parked in the next frame read",
      ).toBe(false);

      await vi.advanceTimersByTimeAsync(279_000);
      expect(
        t.signal()?.reason?.kind,
        "the scene's premise: the deadline's timer fired on time and aborted the request signal with its own reason",
      ).toBe("total_timeout");

      const res = await req;
      expect(
        res.statusCode,
        "the status is sealed at 200 once the first frame is written: the deadline never changes it",
      ).toBe(200);
      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(
        t.capture.byEvent("stream_done")[0],
        "after the first-frame line the deadline's own abort is TOTAL_TIMEOUT, decided before the rejection of the read is classified, never UPSTREAM_DEATH; gateway-fault, the class of the same deadline before the first frame",
      ).toMatchObject({
        completed: false,
        terminal: "TOTAL_TIMEOUT",
        error_class: "gateway-fault",
      });
      expect(
        res.rawPayload,
        "the deadline travels inside the stream: the forwarded frame, then exactly one TOTAL_TIMEOUT sentinel in the byte form of the spec's wire example, and never a fabricated terminating frame",
      ).toStrictEqual(
        Buffer.from(ONE_CONTENT_FRAME + TOTAL_TIMEOUT_SENTINEL, "utf8"),
      );
      expect(
        t.recorded,
        "no failure vote after the first-frame line: the deadline votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });
});

describe("streaming flag ON: after the first-frame line, a frame read past the deadline is never written", () => {
  const TOTAL_TIMEOUT_SENTINEL =
    'data: {"error": {"message": "stream exceeded the total duration limit", "type": "gateway_timeout", "code": "total_timeout_exceeded"}, "gateway": {"terminal": "TOTAL_TIMEOUT"}}\n\n';
  const LATE_FRAME =
    'data: {"id": "c1", "choices": [{"delta": {"content": "late"}}]}\n\n';
  const PAST_DEADLINE_MS = 280_000 + 200;

  // A fake accepted stream for a loop that never waits: with a zero high-water
  // mark the body pulls only while a read is pending, so once `armed`, the
  // loop's next read itself moves the clock past the deadline and finds the
  // late frame ready. No macrotask runs between that read and the loop's
  // decision, so the deadline's timer never gets a turn.
  function busyAcceptedStream(
    signal: AbortSignal,
    pastDeadline: () => void,
  ): {
    stream: AcceptedStream;
    release: (text: string) => void;
    arm: () => void;
    lateFrameServed: () => boolean;
  } {
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
    let armed = false;
    let served = false;

    const body = new ReadableStream<Uint8Array>(
      {
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
        pull(c) {
          if (armed && !served) {
            served = true;
            pastDeadline();
            c.enqueue(new TextEncoder().encode(LATE_FRAME));
          }
        },
      },
      { highWaterMark: 0 },
    );

    return {
      stream: { kind: "accepted_stream", status: 200, reader: body.getReader() },
      release: (text) => {
        if (controller === undefined) {
          throw new Error("the fake stream has not started");
        }
        controller.enqueue(new TextEncoder().encode(text));
      },
      arm: () => {
        armed = true;
      },
      lateFrameServed: () => served,
    };
  }

  async function buildBusyApp(pastDeadline: () => void) {
    const capture = makeLogCapture();
    const violations: string[] = [];
    const { breaker, recorded } = recordingBreaker();
    let busy: ReturnType<typeof busyAcceptedStream> | undefined;
    let receivedSignal: AbortSignal | undefined;

    const app = await buildProxyApp({
      breaker,
      logger: capture.logger,
      streamingEnabled: true,
      upstreamBuffered: bufferedTripwire(violations).seam,
      upstreamStreaming: (_body, signal) => {
        receivedSignal = signal;
        busy = busyAcceptedStream(signal, pastDeadline);
        return Promise.resolve(busy.stream);
      },
    });

    return {
      app,
      capture,
      violations,
      recorded,
      signal: () => receivedSignal,
      busy: () => {
        if (busy === undefined) {
          throw new Error("the streaming upstream was not called");
        }
        return busy;
      },
    };
  }

  it.each([
    {
      // Parked in the frame read; the deadline's timer is late and has not
      // run; a frame arrives after the deadline.
      scene: "the loop is parked, the timer late",
      busy: false,
    },
    {
      // A busy loop reads a frame after the deadline: the frame is ready at
      // the read itself, so the deadline's timer never gets a turn.
      scene: "a busy loop, the timer starved",
      busy: true,
    },
  ])("ends as TOTAL_TIMEOUT without writing the frame when $scene", async ({ busy }) => {
    let startedAt = 0;
    const stepPastDeadline = () => {
      vi.setSystemTime(startedAt + PAST_DEADLINE_MS);
    };
    const parked = await buildDrivenApp();
    const busyApp = await buildBusyApp(stepPastDeadline);
    const t = busy ? busyApp : parked;
    const release = (text: string) =>
      busy ? busyApp.busy().release(text) : parked.driven().release(text);
    vi.useFakeTimers();
    startedAt = Date.now();

    try {
      const req = inject(t.app);
      let ended = false;
      void req.then(() => {
        ended = true;
      });
      await vi.advanceTimersByTimeAsync(1_000);

      if (busy) {
        busyApp.busy().arm();
      }
      release(ONE_CONTENT_FRAME);
      await vi.advanceTimersByTimeAsync(0);

      if (!busy) {
        stepPastDeadline();
        release(LATE_FRAME);
        await vi.advanceTimersByTimeAsync(0);
      }

      expect(t.violations).toStrictEqual([]);
      expect(
        t.capture.byEvent("stream_first_token"),
        "the scene's premise: the first frame crossed the first-frame line before the deadline",
      ).toHaveLength(1);
      if (busy) {
        expect(
          busyApp.busy().lateFrameServed(),
          "the scene's premise: the loop's own read moved the clock past the deadline and found the late frame ready",
        ).toBe(true);
      }
      expect(
        t.signal()?.aborted,
        "the scene's premise: the deadline's timer has not run, so the request signal is not aborted and only the deadline value can decide",
      ).toBe(false);

      expect(
        ended,
        "the deadline is a value read at the decision point: after the frame read, because a reading taken before it ages inside the wait for the frame, and before the write; a frame read past it ends the stream, so the response has ended",
      ).toBe(true);

      const res = await req;
      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(
        t.capture.byEvent("stream_done")[0],
        "a frame read past the deadline does not outrank it: the terminal is TOTAL_TIMEOUT, gateway-fault",
      ).toMatchObject({
        completed: false,
        terminal: "TOTAL_TIMEOUT",
        error_class: "gateway-fault",
      });
      expect(
        res.rawPayload,
        "the frame read past the deadline is never written: the body is the forwarded first frame, then exactly one TOTAL_TIMEOUT sentinel",
      ).toStrictEqual(
        Buffer.from(ONE_CONTENT_FRAME + TOTAL_TIMEOUT_SENTINEL, "utf8"),
      );
      expect(
        t.recorded,
        "the deadline votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
    } finally {
      vi.useRealTimers();
      await parked.app.close();
      await busyApp.app.close();
    }
  });

  it("ends as TOTAL_TIMEOUT without writing a frame read before the deadline's own abort, with the clock stepped back behind the deadline", async () => {
    const t = await buildDrivenApp();
    vi.useFakeTimers();
    const startedAt = Date.now();

    try {
      const req = inject(t.app);
      await vi.advanceTimersByTimeAsync(1_000);
      t.driven().release(ONE_CONTENT_FRAME);
      await vi.advanceTimersByTimeAsync(0);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.capture.byEvent("stream_first_token"),
        "the scene's premise: the first frame crossed the first-frame line before the deadline",
      ).toHaveLength(1);

      // The late frame resolves the pending read; in the same tick, before
      // the loop's continuation runs, the deadline's timer fires and aborts,
      // and the clock steps back behind the deadline value.
      t.driven().release(LATE_FRAME);
      vi.advanceTimersByTime(279_000);
      expect(
        t.signal()?.reason?.kind,
        "the scene's premise: the deadline's timer has fired and aborted the request signal with its own reason",
      ).toBe("total_timeout");
      vi.setSystemTime(startedAt + 279_990);
      await vi.advanceTimersByTimeAsync(0);

      const res = await req;
      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(
        t.capture.byEvent("stream_done")[0],
        "the deadline's own abort ends the stream as TOTAL_TIMEOUT, gateway-fault",
      ).toMatchObject({
        completed: false,
        terminal: "TOTAL_TIMEOUT",
        error_class: "gateway-fault",
      });
      expect(
        res.rawPayload,
        "the deadline's own abort is the deadline at every wake, even with the clock stepped back: a frame read before the abort is never written",
      ).toStrictEqual(
        Buffer.from(ONE_CONTENT_FRAME + TOTAL_TIMEOUT_SENTINEL, "utf8"),
      );
      expect(
        t.recorded,
        "the deadline votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });
});

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
        "the scene's premise: nothing has aborted the request signal yet, so only the destroyed response can decide",
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

describe("streaming flag ON: after the first-frame line, the deadline decides whatever woke the loop", () => {
  const TOTAL_TIMEOUT_SENTINEL =
    'data: {"error": {"message": "stream exceeded the total duration limit", "type": "gateway_timeout", "code": "total_timeout_exceeded"}, "gateway": {"terminal": "TOTAL_TIMEOUT"}}\n\n';
  const LATE_FRAME =
    'data: {"id": "c1", "choices": [{"delta": {"content": "late"}}]}\n\n';
  const RESET_PAST_DEADLINE = Object.assign(new TypeError("terminated"), {
    cause: Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }),
  });
  const WAKE_RULE =
    "nothing that arrives through the wake outranks the deadline";

  type Scene = {
    driven: ReturnType<typeof drivenAcceptedStream>;
    startedAt: number;
  };

  it.each([
    {
      row: "a reset past the deadline",
      act: ({ driven, startedAt }: Scene) => {
        vi.setSystemTime(startedAt + 280_200);
        driven.fail(RESET_PAST_DEADLINE);
      },
      aborted: false,
      noCauseLine: true,
      rule: `${WAKE_RULE}: a reset read past the deadline is TOTAL_TIMEOUT, never UPSTREAM_DEATH`,
    },
    {
      row: "[DONE] past the deadline",
      act: ({ driven, startedAt }: Scene) => {
        vi.setSystemTime(startedAt + 280_200);
        driven.release("data: [DONE]\n\n");
      },
      aborted: false,
      noCauseLine: false,
      rule: `${WAKE_RULE}: a terminating frame after the deadline is not forwarded and never makes it DONE`,
    },
    {
      row: "the cap past the deadline",
      act: ({ driven, startedAt }: Scene) => {
        vi.setSystemTime(startedAt + 280_200);
        driven.release(`data: ${"x".repeat(PARSER_BUFFER_CAP + 1)}`);
      },
      aborted: false,
      noCauseLine: false,
      rule: `${WAKE_RULE}: the reader's cap after the deadline is TOTAL_TIMEOUT with total_timeout_exceeded, never upstream_decode_error`,
    },
    {
      row: "a frame exactly at the deadline",
      act: ({ driven, startedAt }: Scene) => {
        vi.setSystemTime(startedAt + 280_000);
        driven.release(LATE_FRAME);
      },
      aborted: false,
      noCauseLine: false,
      rule: "`>=` at the boundary: an ordinary frame read with the clock exactly at the deadline is not written",
    },
    {
      row: "the abort, clock stepped back",
      act: ({ startedAt }: Scene) => {
        vi.advanceTimersByTime(279_000);
        vi.setSystemTime(startedAt + 279_995);
      },
      aborted: true,
      noCauseLine: false,
      rule: "the deadline's own abort is the deadline, even with the clock stepped back",
    },
  ])("ends as TOTAL_TIMEOUT with one sentinel when the loop wakes with $row", async ({ act, aborted, noCauseLine, rule }) => {
    const t = await buildDrivenApp();
    vi.useFakeTimers();
    const startedAt = Date.now();

    try {
      const req = inject(t.app);
      let ended = false;
      void req.then(() => {
        ended = true;
      });
      await vi.advanceTimersByTimeAsync(1_000);
      t.driven().release(ONE_CONTENT_FRAME);
      await vi.advanceTimersByTimeAsync(0);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.capture.byEvent("stream_first_token"),
        "the scene's premise: the first frame crossed the first-frame line before the deadline",
      ).toHaveLength(1);

      act({ driven: t.driven(), startedAt });
      expect(
        t.signal()?.reason?.kind,
        aborted
          ? "the scene's premise: the deadline's timer fired on time and aborted the request signal with its own reason"
          : "the scene's premise: the deadline's timer is late and has not run, so only the deadline value can decide",
      ).toBe(aborted ? "total_timeout" : undefined);
      await vi.advanceTimersByTimeAsync(0);

      expect(ended, rule).toBe(true);
      const res = await req;
      expect(
        res.statusCode,
        "the status is sealed at 200 once the first frame is written",
      ).toBe(200);
      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(t.capture.byEvent("stream_done")[0], rule).toMatchObject({
        completed: false,
        terminal: "TOTAL_TIMEOUT",
        error_class: "gateway-fault",
      });
      expect(res.rawPayload, rule).toStrictEqual(
        Buffer.from(ONE_CONTENT_FRAME + TOTAL_TIMEOUT_SENTINEL, "utf8"),
      );
      expect(
        t.recorded,
        "the deadline votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
      if (noCauseLine) {
        // A net stated as a negation on purpose: the cause line is the
        // classifier's footprint, and the classifier never runs once the
        // deadline check holds.
        expect(
          t.capture.logs.filter((log) => "cause_code" in log),
          "the classifier runs only after the deadline check: a deadline end logs no upstream cause",
        ).toStrictEqual([]);
      }
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });
});

describe("streaming flag ON: after the first-frame line, a rejection the classifier does not recognize ends the stream", () => {
  const UNRECOGNIZED_REJECTION_SENTINEL =
    'data: {"error": {"message": "stream ended by an internal error", "type": "internal_error", "code": "unhandled_exception"}, "gateway": {"terminal": "UNRECOGNIZED_REJECTION"}}\n\n';
  const NO_CAUSE_REJECTION = new Error("the upstream read failed");

  it("ends as UNRECOGNIZED_REJECTION when the read rejects with an error that carries no cause code and no cause message", async () => {
    const t = await buildDrivenApp();
    vi.useFakeTimers();

    try {
      const req = inject(t.app);
      let ended = false;
      void req.then(() => {
        ended = true;
      });
      await vi.advanceTimersByTimeAsync(1_000);
      t.driven().release(ONE_CONTENT_FRAME);
      await vi.advanceTimersByTimeAsync(0);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.capture.byEvent("stream_first_token"),
        "the scene's premise: the first frame crossed the first-frame line",
      ).toHaveLength(1);

      t.driven().fail(NO_CAUSE_REJECTION);
      await vi.advanceTimersByTimeAsync(0);
      expect(
        t.signal()?.aborted,
        "the scene's premise: nothing aborted the request signal, so checks (1) and (2) do not hold",
      ).toBe(false);

      expect(
        ended,
        "a rejection the classifier does not recognize still ends the stream: the loop never leaves without a terminal",
      ).toBe(true);
      const res = await req;
      expect(
        res.statusCode,
        "the status is sealed at 200 once the first frame is written",
      ).toBe(200);
      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(
        t.capture.byEvent("stream_done")[0],
        "a rejection the classifier does not recognize is never an upstream death: UNRECOGNIZED_REJECTION, gateway-fault",
      ).toMatchObject({
        completed: false,
        terminal: "UNRECOGNIZED_REJECTION",
        error_class: "gateway-fault",
      });
      expect(
        res.rawPayload,
        "the stream ends with the forwarded frame and exactly one UNRECOGNIZED_REJECTION sentinel in the byte form of the spec's wire example",
      ).toStrictEqual(
        Buffer.from(ONE_CONTENT_FRAME + UNRECOGNIZED_REJECTION_SENTINEL, "utf8"),
      );
      expect(
        t.recorded,
        "an unrecognized rejection votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);
      expect(
        t.capture.logs
          .filter((log) => typeof log.level === "number" && log.level >= 40)
          .map((log) => ({ msg: log.msg, abort_identity: log.abort_identity })),
        "the classifier's boundary line is logged once, and nothing is thrown to the framework's error handler: no other warning or error line",
      ).toStrictEqual([
        { msg: "Unrecognized upstream rejection", abort_identity: false },
      ]);
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });
});
