import http from "node:http";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

// An upstream 3xx at the real boundary: the real route, the real adapter and
// a local upstream. A fake adapter cannot show that nothing of the upstream's
// answer reaches the consumer or the logs, because it never carried any of it.
// The gateway's modules are reloaded for every cell because the redirect alert
// fires once per process.

const SANITIZED_REDIRECT_BODY = {
  error: {
    message: "upstream redirect blocked",
    type: "server_error",
    code: "upstream_redirect_blocked",
  },
};

// Values that identify the upstream's answer: its body and the three parts of
// its Location.
const BODY_CANARY = "upstream-body-canary-5f2a";
const HOST_CANARY = "secret-host-canary-5f2a.invalid";
const PATH_CANARY = "secret-path-canary-5f2a";
const QUERY_CANARY = "secret-query-canary-5f2a";
const LOCATION = `http://${HOST_CANARY}/${PATH_CANARY}?key=${QUERY_CANARY}`;

// Fields the logger adds to every line; they are not part of an event.
const LOGGER_FIELDS = ["hostname", "level", "pid", "reqId", "time"];

function eventFields(line: CapturedLog | undefined): string[] {
  return Object.keys(line ?? {})
    .filter((key) => !LOGGER_FIELDS.includes(key))
    .sort();
}

const reqBody = {
  model: "gpt-4o-mini",
  messages: [{ role: "user", content: "hi" }],
};

async function requestThroughGateway(
  upstreamAnswer: (res: http.ServerResponse) => void,
  stream: boolean,
) {
  let answersSent = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      upstreamAnswer(res);
      answersSent += 1;
    });
  });
  const upstream = await listenEphemeral(server);
  const { createOpenAIClient } = await import("../../src/upstream/openai.js");
  const { buildProxyApp } = await import("../helpers/proxy-app.js");
  const client = createOpenAIClient({
    apiKey: "deployment-key",
    baseURL: `http://127.0.0.1:${upstream.port}`,
  });
  const capture = makeLogCapture();
  const app = await buildProxyApp({
    breaker: stubBreaker,
    streamingEnabled: true,
    logger: capture.logger,
    upstreamBuffered: client.buffered,
    upstreamStreaming: client.streaming,
  });
  try {
    const response = await app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer(), "content-type": "application/json" },
      payload: { ...reqBody, stream },
    });
    return { response, capture, answersSent: () => answersSent };
  } finally {
    await app.close();
    server.closeAllConnections();
    await upstream.close();
  }
}

const redirectAnswer = (res: http.ServerResponse): void => {
  res.writeHead(302, { location: LOCATION, "content-type": "text/plain" });
  res.end(BODY_CANARY);
};

beforeEach(() => {
  vi.resetModules();
});

describe("upstream 3xx at the real boundary — what the consumer receives", () => {
  it("answers a buffered 302 with the sanitized 502 and nothing of the upstream's answer", async () => {
    const { response, answersSent } = await requestThroughGateway(
      redirectAnswer,
      false,
    );

    // Positive control: the upstream did send its body and its Location, so
    // their absence below is a reading and not a blind ruler.
    expect(answersSent(), "the scene needs the upstream's 302 to be sent").toBe(1);
    expect(
      {
        status: response.statusCode,
        errorClass: response.headers["x-gateway-error-class"],
        body: JSON.parse(response.payload) as unknown,
      },
      "a 3xx is the operator's fault: the consumer receives the gateway's own 502, whole and exact",
    ).toStrictEqual({
      status: 502,
      errorClass: "upstream-redirect-blocked",
      body: SANITIZED_REDIRECT_BODY,
    });
    expect(
      Object.keys(response.headers).sort(),
      "the response carries the gateway's own headers and no header of the upstream, Location included",
    ).toStrictEqual([
      "connection",
      "content-length",
      "content-type",
      "date",
      "x-gateway-error-class",
    ]);
  });
});

// The four 3xx statuses fetch does not treat as a redirect under any option.
const NOT_A_REDIRECT_CELLS = [300, 304, 305, 306].flatMap((status) => [
  { status, call: "buffered", stream: false },
  { status, call: "streaming", stream: true },
]);

describe("upstream 3xx at the real boundary — a 3xx that is not a redirect", () => {
  it.each(NOT_A_REDIRECT_CELLS)(
    "$call $status: the consumer gets the sanitized 502 and the operator gets the alert",
    async ({ status, stream }) => {
      const { response, capture, answersSent } = await requestThroughGateway(
        (res) => {
          res.writeHead(status, { "content-type": "text/plain" });
          res.end(BODY_CANARY);
        },
        stream,
      );

      expect(answersSent(), "the scene needs the upstream's answer to be sent").toBe(1);
      expect(
        {
          status: response.statusCode,
          errorClass: response.headers["x-gateway-error-class"],
          body: JSON.parse(response.payload) as unknown,
        },
        "a 3xx is never relayed to the consumer, redirect or not: the answer is the gateway's own 502",
      ).toStrictEqual({
        status: 502,
        errorClass: "upstream-redirect-blocked",
        body: SANITIZED_REDIRECT_BODY,
      });
      expect(
        capture.byEvent("operational_alert").map((line) => line.alert),
        "a 3xx that fetch would not follow is still the operator's fault: it raises the redirect alert, once",
      ).toStrictEqual(["upstream_redirect_blocked"]);
      expect(
        capture.byEvent("req_complete").map((line) => ({
          terminal: line.terminal,
          upstream_status: line.upstream_status,
        })),
        "the request ends as REDIRECT_BLOCKED and its line says which status the upstream answered",
      ).toStrictEqual([{ terminal: "REDIRECT_BLOCKED", upstream_status: status }]);
    },
  );
});

describe("upstream 3xx at the real boundary — what the logs carry", () => {
  it.each([
    { call: "buffered", stream: false },
    { call: "streaming", stream: true },
  ])(
    "$call 302: the status is logged as a number, and nothing of Location reaches a line",
    async ({ stream }) => {
      const { capture, answersSent } = await requestThroughGateway(
        redirectAnswer,
        stream,
      );

      expect(answersSent(), "the scene needs the upstream's 302 to be sent").toBe(1);
      const complete = capture.byEvent("req_complete");
      expect(complete, "the request writes one req_complete").toHaveLength(1);
      expect(
        complete[0]?.upstream_status,
        "a blocked 3xx logs the upstream's status as a number, so the operator can tell a 302 from a 304",
      ).toBe(302);
      expect(
        eventFields(complete[0]),
        "req_complete of a blocked 3xx carries exactly these fields",
      ).toStrictEqual([
        "attempts",
        "duration_ms",
        "error_class",
        "event",
        "gateway_overhead_ms",
        "req_id",
        "retry_disposition",
        "status",
        "stream",
        "terminal",
        "upstream_duration_ms",
        "upstream_status",
      ]);

      const alerts = capture.byEvent("operational_alert");
      expect(
        alerts.map((line) => eventFields(line)),
        "the alert line carries only its allowlisted fields",
      ).toStrictEqual([["alert", "event", "req_id"]]);

      // A net, not the ruler: the fields above are the allowlist; this only
      // catches a value of Location that slipped into any line by another way.
      const everyLine = JSON.stringify(capture.logs);
      for (const canary of [HOST_CANARY, PATH_CANARY, QUERY_CANARY, BODY_CANARY]) {
        expect(
          everyLine.includes(canary),
          `net: no part of the upstream's answer may reach a log line (${canary})`,
        ).toBe(false);
      }
    },
  );

  it("an upstream 500 passed through: req_complete carries exactly today's fields, without upstream_status", async () => {
    const { capture, answersSent } = await requestThroughGateway((res) => {
      res.writeHead(500, { "content-type": "application/json" });
      res.end('{"error":{"message":"boom"}}');
    }, false);

    expect(answersSent(), "the scene needs the upstream's 500 to be sent").toBe(1);
    const complete = capture.byEvent("req_complete");
    expect(complete, "the request writes one req_complete").toHaveLength(1);
    expect(
      eventFields(complete[0]),
      "upstream_status belongs to the blocked 3xx alone: every other terminal keeps its fields",
    ).toStrictEqual([
      "attempts",
      "duration_ms",
      "error_class",
      "event",
      "gateway_overhead_ms",
      "req_id",
      "retry_disposition",
      "status",
      "stream",
      "terminal",
      "upstream_duration_ms",
    ]);
  });
});
