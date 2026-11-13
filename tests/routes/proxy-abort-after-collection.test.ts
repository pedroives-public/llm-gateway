import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { STREAM_TOTAL_DURATION_MS } from "../../src/config.js";
import type {
  CircuitBreaker,
  RequestOutcome,
} from "../../src/reliability/circuit-breaker.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { bearer } from "../helpers/fake-auth-db.js";
import {
  abortListeners,
  collectThroughWindow,
  newCanary,
  settlesWithin,
  sleep,
  waitFor,
} from "../helpers/garbage-collection.js";
import {
  wrapGlobalFetch,
  type RedirectOption,
} from "../helpers/global-fetch-wrapper.js";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

// Tenant requests through the real route and the real adapter, on the
// runtime's own fetch. The upstream sends a head and then stalls; a garbage
// collection runs while the gateway reads the body; then the route's own
// clock expires or the client leaves. Nothing but the abort can end the read.

// The buffered route's wall clock (a constant of the route, not exported).
const BUFFERED_WALL_CLOCK_MS = 30_000;

const HEAD_DEADLINE_MS = 2_000;
// Lets the gateway reach the body read after the upstream's head arrives.
const READ_STARTED_SETTLE_MS = 150;
const REQUEST_END_DEADLINE_MS = 2_000;
const SOCKET_CLOSE_DEADLINE_MS = 2_000;
const EVENT_DEADLINE_MS = 2_000;
// How long the control arm watches a request the abort did not reach. Under
// redirect "manual" a request ended at most 18 ms after its clock, and the
// upstream connection closed at most 22 ms after the client left (3 runs of
// each scene, Node 22.22.3, 20 ms poll step); the window is over twenty times
// that, so a read the abort did reach would have ended inside it.
const HELD_REQUEST_OBSERVATION_MS = 500;

type Behavior =
  | "json-200-stall"
  | "error-500-stall"
  | "sse-comment-stall"
  | "sse-frame-stall";

const reqBody = {
  model: "gpt-4o-mini",
  messages: [{ role: "user", content: "hi" }],
};

async function startUpstream(behavior: Behavior) {
  let headsSent = 0;
  let socketsClosed = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.on("close", () => {
        socketsClosed += 1;
      });
      switch (behavior) {
        case "json-200-stall":
          res.writeHead(200, { "content-type": "application/json" });
          res.write('{"id":"chatcmpl-x","choices":[');
          break;
        case "error-500-stall":
          res.writeHead(500, { "content-type": "application/json" });
          res.write('{"error":{"message":"partial');
          break;
        case "sse-comment-stall":
          res.writeHead(200, { "content-type": "text/event-stream" });
          res.write(": open\n\n");
          break;
        case "sse-frame-stall":
          res.writeHead(200, { "content-type": "text/event-stream" });
          res.write(
            'data: {"id":"x","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n',
          );
          break;
      }
      headsSent += 1;
    });
  });
  const upstream = await listenEphemeral(server);
  return {
    port: upstream.port,
    headsSent: () => headsSent,
    socketsClosed: () => socketsClosed,
    close: async () => {
      server.closeAllConnections();
      await upstream.close();
    },
  };
}

async function buildScene(behavior: Behavior, forceRedirect: RedirectOption) {
  const upstream = await startUpstream(behavior);
  const wrapper = wrapGlobalFetch(forceRedirect);
  const client = createOpenAIClient({
    apiKey: "deployment-key",
    baseURL: `http://127.0.0.1:${upstream.port}`,
  });
  let handedSignal: AbortSignal | undefined;
  const votes: RequestOutcome[] = [];
  const recordingBreaker: CircuitBreaker = {
    tryAcquire: () => ({ kind: "NORMAL" }),
    recordResult: (outcome) => {
      votes.push(outcome);
    },
    getState: () => "CLOSED",
    close: () => {},
  };
  const capture = makeLogCapture();
  const app = await buildProxyApp({
    breaker: recordingBreaker,
    streamingEnabled: true,
    logger: capture.logger,
    upstreamBuffered: (body, signal, log) => {
      handedSignal = signal;
      return client.buffered(body, signal, log);
    },
    upstreamStreaming: (body, signal, log) => {
      handedSignal = signal;
      return client.streaming(body, signal, log);
    },
  });
  return {
    app,
    upstream,
    wrapper,
    votes,
    // The request's terminal events, in the order they were written.
    terminalEvents: (): CapturedLog[] =>
      capture.logs.filter(
        (log) => log.event === "req_complete" || log.event === "stream_done",
      ),
    // Abort listeners on the signal the route handed to the adapter.
    abortListeners: () => abortListeners(handedSignal),
  };
}

// The code of the gateway's error: the whole body of a JSON answer, or the
// last data line of a stream that the gateway ended with its own sentinel.
function gatewayErrorCode(payload: string): unknown {
  const lastDataLine = payload
    .split("\n")
    .filter((line) => line.startsWith("data: "))
    .at(-1);
  const parsed: unknown = JSON.parse(
    lastDataLine === undefined ? payload : lastDataLine.slice("data: ".length),
  );
  if (
    typeof parsed === "object" &&
    parsed !== null &&
    "error" in parsed &&
    typeof parsed.error === "object" &&
    parsed.error !== null &&
    "code" in parsed.error
  ) {
    return parsed.error.code;
  }
  return undefined;
}

// The same scene under two redirect options. "manual" is what the adapter
// writes; "error" is forced by the wrapper around the global fetch and is the
// control arm: it reproduces the abort the runtime loses after a collection.
const arms = [
  {
    option: "manual",
    forceRedirect: undefined,
    listenersAfterCollection: 1,
    listenerRule:
      'under redirect "manual" the runtime keeps its abort listener across a collection, so an abort still reaches the read',
    afterAbort: "ends",
  },
  {
    option: "error",
    forceRedirect: "error",
    listenersAfterCollection: 0,
    listenerRule:
      'control arm: under redirect "error" a collection removes the runtime\'s abort listener; if it stays, the arm no longer reproduces the lost abort',
    afterAbort: "stays held",
  },
] as const;

const SENT_REDIRECT_RULE =
  'the adapter wrote redirect "manual" in one call: the arms differ only by what the wrapper forces';
const LISTENER_BEFORE_RULE =
  "before the collection the signal the route handed the adapter carries one abort listener, the runtime's own";

// What each scene answers when its clock ends the read. A buffered request
// writes no terminal name, so those two rows fix the event by its status.
const clockScenes = [
  {
    scene: "buffered 2xx",
    stream: false,
    behavior: "json-200-stall",
    status: 504,
    code: "wall_clock_exceeded",
    terminalEvent: { event: "req_complete", status: 504 },
  },
  {
    scene: "buffered 500 body",
    stream: false,
    behavior: "error-500-stall",
    status: 504,
    code: "wall_clock_exceeded",
    terminalEvent: { event: "req_complete", status: 504 },
  },
  {
    scene: "stream before the first frame",
    stream: true,
    behavior: "sse-comment-stall",
    status: 504,
    code: "total_timeout_exceeded",
    terminalEvent: { event: "req_complete", terminal: "TOTAL_TIMEOUT" },
  },
  {
    scene: "stream after the first frame",
    stream: true,
    behavior: "sse-frame-stall",
    status: 200,
    code: "total_timeout_exceeded",
    terminalEvent: { event: "stream_done", terminal: "TOTAL_TIMEOUT" },
  },
  {
    scene: "stream 500 body",
    stream: true,
    behavior: "error-500-stall",
    status: 504,
    code: "total_timeout_exceeded",
    terminalEvent: { event: "req_complete", terminal: "TOTAL_TIMEOUT" },
  },
] as const;

const clockCells = clockScenes.flatMap((scene) =>
  arms.map((arm) => ({ ...scene, ...arm })),
);

describe("tenant request — the route's clock after a garbage collection", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it.each(clockCells)(
    '$scene, redirect "$option": at its clock the request $afterAbort',
    async ({
      stream,
      behavior,
      status,
      code,
      terminalEvent,
      forceRedirect,
      listenersAfterCollection,
      listenerRule,
      afterAbort,
    }) => {
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
      const scene = await buildScene(behavior, forceRedirect);
      const canary = newCanary();
      let ended = false;
      const pending = Promise.resolve(
        scene.app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: {
            authorization: bearer(),
            "content-type": "application/json",
          },
          payload: { ...reqBody, stream },
        }),
      ).finally(() => {
        ended = true;
      });

      try {
        await waitFor(
          () => scene.upstream.headsSent() === 1,
          HEAD_DEADLINE_MS,
          "the upstream never sent its head",
        );
        await sleep(READ_STARTED_SETTLE_MS);
        const listenersBeforeCollection = scene.abortListeners();

        await collectThroughWindow(canary);

        expect(scene.wrapper.sentRedirects, SENT_REDIRECT_RULE).toStrictEqual([
          "manual",
        ]);
        expect(listenersBeforeCollection, LISTENER_BEFORE_RULE).toBe(1);
        expect(scene.abortListeners(), listenerRule).toBe(
          listenersAfterCollection,
        );

        await vi.advanceTimersByTimeAsync(
          stream ? STREAM_TOTAL_DURATION_MS : BUFFERED_WALL_CLOCK_MS,
        );

        if (afterAbort === "ends") {
          const response = await settlesWithin(
            pending,
            REQUEST_END_DEADLINE_MS,
            "the request never ended after its clock: the body read still holds it",
          );
          expect(
            {
              status: response.statusCode,
              code: gatewayErrorCode(response.payload),
            },
            "the clock ends the request with the gateway's own timeout answer",
          ).toStrictEqual({ status, code });
          expect(
            scene.votes,
            "a clock expiry proves the request outlived its budget, not an upstream failure: one INCONCLUSIVE vote",
          ).toStrictEqual(["INCONCLUSIVE"]);
          expect(
            scene.terminalEvents(),
            "the request writes exactly one terminal event",
          ).toHaveLength(1);
          expect(scene.terminalEvents()[0]).toMatchObject(terminalEvent);
          await waitFor(
            () => scene.upstream.socketsClosed() === 1,
            SOCKET_CLOSE_DEADLINE_MS,
            "the abort did not close the upstream connection",
          );
        } else {
          await sleep(HELD_REQUEST_OBSERVATION_MS);
          expect(
            ended,
            "control arm: with the listener gone, the abort does not reach the body read and the request stays held through the window",
          ).toBe(false);
          expect(
            scene.votes,
            "control arm: a held request has not voted",
          ).toStrictEqual([]);
          expect(
            scene.upstream.socketsClosed(),
            "control arm: the upstream connection stays open through the window",
          ).toBe(0);
        }
      } finally {
        vi.useRealTimers();
        // Closing the upstream ends a read the abort did not reach, so the
        // request finishes and the app can close.
        await scene.upstream.close();
        await settlesWithin(
          pending.catch(() => undefined),
          REQUEST_END_DEADLINE_MS,
          "the request did not end after its upstream closed",
        );
        await scene.app.close();
      }
    },
    20_000,
  );
});

// What each scene writes when the client's departure ends the upstream read.
const clientGoneScenes = [
  {
    scene: "before the first frame",
    behavior: "sse-comment-stall",
    terminalEvent: {
      event: "req_complete",
      status: 499,
      error_class: "client-fault",
      terminal: "CLIENT_GONE",
    },
  },
  {
    scene: "after the first frame",
    behavior: "sse-frame-stall",
    terminalEvent: {
      event: "stream_done",
      error_class: "client-fault",
      terminal: "CLIENT_GONE",
    },
  },
  {
    scene: "during a 500 body",
    behavior: "error-500-stall",
    terminalEvent: {
      event: "req_complete",
      status: 499,
      error_class: "client-fault",
      terminal: "CLIENT_GONE",
    },
  },
] as const;

const clientGoneCells = clientGoneScenes.flatMap((scene) =>
  arms.map((arm) => ({ ...scene, ...arm })),
);

describe("tenant stream — the client leaves after a garbage collection", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each(clientGoneCells)(
    'client gone $scene, redirect "$option": the upstream read $afterAbort',
    async ({
      behavior,
      terminalEvent,
      forceRedirect,
      listenersAfterCollection,
      listenerRule,
      afterAbort,
    }) => {
      const scene = await buildScene(behavior, forceRedirect);
      const canary = newCanary();
      await scene.app.listen({ port: 0, host: "127.0.0.1" });
      const address = scene.app.server.address();
      const port =
        typeof address === "object" && address !== null ? address.port : 0;

      try {
        let firstChunkSeen = false;
        const request = http.request({
          host: "127.0.0.1",
          port,
          method: "POST",
          path: "/v1/chat/completions",
          headers: {
            authorization: bearer(),
            "content-type": "application/json",
          },
        });
        request.on("response", (response) => {
          response.on("data", () => {
            firstChunkSeen = true;
          });
        });
        request.on("error", () => {});
        request.end(JSON.stringify({ ...reqBody, stream: true }));

        await waitFor(
          () => scene.upstream.headsSent() === 1,
          HEAD_DEADLINE_MS,
          "the upstream never sent its head",
        );
        if (behavior === "sse-frame-stall") {
          await waitFor(
            () => firstChunkSeen,
            HEAD_DEADLINE_MS,
            "the client never received the first frame",
          );
        }
        await sleep(READ_STARTED_SETTLE_MS);
        const listenersBeforeCollection = scene.abortListeners();

        await collectThroughWindow(canary);

        expect(scene.wrapper.sentRedirects, SENT_REDIRECT_RULE).toStrictEqual([
          "manual",
        ]);
        expect(listenersBeforeCollection, LISTENER_BEFORE_RULE).toBe(1);
        expect(scene.abortListeners(), listenerRule).toBe(
          listenersAfterCollection,
        );

        request.destroy();

        if (afterAbort === "ends") {
          await waitFor(
            () => scene.upstream.socketsClosed() === 1,
            SOCKET_CLOSE_DEADLINE_MS,
            "the client's departure did not close the upstream connection",
          );
          await waitFor(
            () => scene.terminalEvents().length === 1,
            EVENT_DEADLINE_MS,
            "the request wrote no terminal event after the client left",
          );
          expect(
            scene.terminalEvents()[0],
            "a departed client is a client fault with the CLIENT_GONE terminal",
          ).toMatchObject(terminalEvent);
          expect(
            scene.votes,
            "a client's departure says nothing about the upstream: one INCONCLUSIVE vote",
          ).toStrictEqual(["INCONCLUSIVE"]);
        } else {
          await sleep(HELD_REQUEST_OBSERVATION_MS);
          expect(
            scene.upstream.socketsClosed(),
            "control arm: with the listener gone, the abort does not reach the upstream read and its connection stays open through the window",
          ).toBe(0);
          expect(
            scene.terminalEvents(),
            "control arm: a held request has written no terminal event",
          ).toStrictEqual([]);
          expect(
            scene.votes,
            "control arm: a held request has not voted",
          ).toStrictEqual([]);
        }
      } finally {
        await scene.upstream.close();
        await scene.app.close();
      }
    },
    20_000,
  );
});
