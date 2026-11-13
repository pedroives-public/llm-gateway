import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { isAcceptedStream } from "../../src/upstream/stream.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import {
  abortListeners,
  collectThroughWindow,
  newCanary,
  sleep,
  waitFor,
} from "../helpers/garbage-collection.js";
import {
  wrapGlobalFetch,
  type RedirectOption,
} from "../helpers/global-fetch-wrapper.js";

// The adapter alone on the runtime's own fetch: a streaming call whose
// upstream sends an SSE head and one comment, then stalls. No clock and no
// cancel run in this scene, so only the caller's abort can end the pending
// read.

const silentLog = { error: () => {}, info: () => {} };
const reqBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
  stream: true,
};

// Real-time bound for the abort to end the pending read and close the
// connection. Under redirect "manual" the read rejected at most 28 ms after
// the abort (5 runs, Node 22.22.3, 20 ms poll step); the deadline is over
// thirty times that.
const ABORT_REACHES_READ_DEADLINE_MS = 1_000;
// How long the control arm watches a read the abort did not reach. Over
// fifteen times the 28 ms above, so a read the abort did reach would have
// ended inside the window.
const HELD_READ_OBSERVATION_MS = 500;

async function startStalledSseUpstream() {
  let socketsClosed = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.on("close", () => {
        socketsClosed += 1;
      });
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write(": open\n\n");
    });
  });
  const upstream = await listenEphemeral(server);
  return {
    baseURL: `http://127.0.0.1:${upstream.port}`,
    socketsClosed: () => socketsClosed,
    close: async () => {
      server.closeAllConnections();
      await upstream.close();
    },
  };
}

// The same scene under two redirect options. "manual" is what the adapter
// writes; "error" is forced by the wrapper around the global fetch and is the
// control arm: it reproduces the abort the runtime loses after a collection.
const arms: ReadonlyArray<{
  option: "manual" | "error";
  forceRedirect: RedirectOption;
  listenersAfterCollection: number;
  listenerRule: string;
  readAfterAbort: "rejects with the abort reason" | "stays pending";
}> = [
  {
    option: "manual",
    forceRedirect: undefined,
    listenersAfterCollection: 1,
    listenerRule:
      'under redirect "manual" the runtime keeps its abort listener across a collection',
    readAfterAbort: "rejects with the abort reason",
  },
  {
    option: "error",
    forceRedirect: "error",
    listenersAfterCollection: 0,
    listenerRule:
      'control arm: under redirect "error" a collection removes the runtime\'s abort listener; if it stays, the arm no longer reproduces the lost abort',
    readAfterAbort: "stays pending",
  },
];

describe("openai streaming client — an abort after a garbage collection", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each(arms)(
    'redirect "$option": the read pending at the caller\'s abort $readAfterAbort',
    async ({
      forceRedirect,
      listenersAfterCollection,
      listenerRule,
      readAfterAbort,
    }) => {
      const wrapper = wrapGlobalFetch(forceRedirect);
      const upstream = await startStalledSseUpstream();
      const caller = new AbortController();
      const client = createOpenAIClient({
        apiKey: "deployment-key",
        baseURL: upstream.baseURL,
      });
      const canary = newCanary();
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;

      try {
        const accepted = await client.streaming(
          reqBody,
          caller.signal,
          silentLog,
        );
        if (!isAcceptedStream(accepted)) {
          throw new Error(
            `the scene needs an accepted stream, got ${accepted.kind}`,
          );
        }
        reader = accepted.reader;
        // The comment the upstream already sent; the next read stays pending.
        await reader.read();
        let settled: { value: unknown } | undefined;
        void reader.read().then(
          () => {
            settled = { value: "resolved" };
          },
          (reason: unknown) => {
            settled = { value: reason };
          },
        );
        const listenersBeforeCollection = abortListeners(caller.signal);

        await collectThroughWindow(canary);

        expect(
          wrapper.sentRedirects,
          'the adapter wrote redirect "manual" in one call: the arms differ only by what the wrapper forces',
        ).toStrictEqual(["manual"]);
        expect(
          listenersBeforeCollection,
          "before the collection the caller's signal carries one abort listener, the runtime's own",
        ).toBe(1);
        expect(abortListeners(caller.signal), listenerRule).toBe(
          listenersAfterCollection,
        );

        const reason = new Error("the caller gave up");
        caller.abort(reason);

        if (readAfterAbort === "rejects with the abort reason") {
          await waitFor(
            () => settled !== undefined,
            ABORT_REACHES_READ_DEADLINE_MS,
            "the pending read did not end at the caller's abort",
          );
          expect(
            settled?.value,
            "the read pending at the abort rejects with the caller's abort reason",
          ).toBe(reason);
          await waitFor(
            () => upstream.socketsClosed() === 1,
            ABORT_REACHES_READ_DEADLINE_MS,
            "the abort did not close the upstream connection",
          );
        } else {
          await sleep(HELD_READ_OBSERVATION_MS);
          expect(
            settled,
            "control arm: with the listener gone, the abort does not reach the read, which stays pending through the window",
          ).toBeUndefined();
          expect(
            upstream.socketsClosed(),
            "control arm: the upstream connection stays open through the window",
          ).toBe(0);
        }
      } finally {
        // Cancelling the reader ends a read the abort did not reach.
        await reader?.cancel().catch(() => {});
        await upstream.close();
      }
    },
  );
});
