import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PROBE_CLOCK_MS } from "../../src/config.js";
import { createProbeRunner } from "../../src/reliability/breaker-probe.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
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

// Keepalive cadence of the local upstream below (real time).
const KEEPALIVE_EVERY_MS = 200;
// Real-time bounds for the waits of the scene; each fails with its own
// sentence, under Vitest's test timeout set on the cell.
const HEAD_DEADLINE_MS = 2_000;
const PROBE_END_DEADLINE_MS = 2_000;
const SOCKET_CLOSE_DEADLINE_MS = 2_000;
// How long the control arm watches an attempt the abort did not reach. Under
// redirect "manual" the attempt ended at most 4 ms after its clock (3 runs,
// Node 22.22.3); the window is over a hundred times that, so an attempt the
// abort did reach would have ended inside it.
const HELD_ATTEMPT_OBSERVATION_MS = 500;

type UpstreamMode = "keepalive" | "stalled" | "error-body-stalled";

// The real adapter, the real runner and a local upstream that never finishes
// its reply. "keepalive" and "stalled" send a 2xx SSE head and then keepalive
// comments or nothing, never [DONE]; "error-body-stalled" sends a 500 head
// and part of an error body. Only setTimeout and clearTimeout are faked, so
// the probe's clock moves on demand while sockets, the upstream's keepalives
// and the garbage collector stay real.
async function startScene(mode: UpstreamMode) {
  let headsSent = 0;
  let socketsClosed = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      if (mode === "error-body-stalled") {
        res.writeHead(500, { "content-type": "application/json" });
        res.write('{"error":{"message":"partial');
      } else {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write(": open\n\n");
      }
      headsSent += 1;
      const keepalive =
        mode === "keepalive"
          ? setInterval(() => res.write(": keepalive\n\n"), KEEPALIVE_EVERY_MS)
          : undefined;
      res.on("close", () => {
        clearInterval(keepalive);
        socketsClosed += 1;
      });
    });
  });
  const upstream = await listenEphemeral(server);
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });

  const adapter = createOpenAIClient({
    apiKey: "deployment-key",
    baseURL: `http://127.0.0.1:${upstream.port}`,
  });
  let probeSignal: AbortSignal | undefined;
  const vote = vi.fn();
  const log = { info: vi.fn(), error: vi.fn() };
  const runProbe = createProbeRunner({
    streaming: (body, signal, adapterLog) => {
      probeSignal = signal;
      return adapter.streaming(body, signal, adapterLog);
    },
    model: "probe-model",
    log,
  });

  return {
    vote,
    log,
    start: () => runProbe(vote, new AbortController().signal),
    headsSent: () => headsSent,
    socketsClosed: () => socketsClosed,
    // Abort listeners on the signal the probe handed to the runtime's fetch.
    abortListeners: () => abortListeners(probeSignal),
    close: async () => {
      vi.useRealTimers();
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
}> = [
  {
    option: "manual",
    forceRedirect: undefined,
    listenersAfterCollection: 2,
    listenerRule:
      'under redirect "manual" the runtime keeps its abort listener across a collection, so an abort still reaches the read',
  },
  {
    option: "error",
    forceRedirect: "error",
    listenersAfterCollection: 1,
    listenerRule:
      'control arm: under redirect "error" a collection removes the runtime\'s abort listener; if it stays, the arm no longer reproduces the lost abort',
  },
];

// With keepalives, a chunk after the clock also wakes the probe's read loop;
// a stalled body sends nothing, so only the cancel can end the read.
const modes = ["keepalive", "stalled"] as const;
const cells = arms.flatMap((arm) => modes.map((mode) => ({ ...arm, mode })));

describe("breaker probe — real runtime, after a garbage collection", () => {
  let close: (() => Promise<void>) | undefined;

  afterEach(async () => {
    await close?.();
    close = undefined;
    vi.unstubAllGlobals();
  });

  it.each(cells)(
    'redirect "$option", $mode upstream: ends at its clock by the reader cancel',
    async ({ mode, forceRedirect, listenersAfterCollection, listenerRule }) => {
      const wrapper = wrapGlobalFetch(forceRedirect);
      const scene = await startScene(mode);
      close = scene.close;
      const canary = newCanary();

      const pendingProbe = scene.start();
      await waitFor(() => scene.headsSent() === 1, HEAD_DEADLINE_MS, "the upstream never sent the probe's head");
      await sleep(KEEPALIVE_EVERY_MS);
      const listenersBeforeCollection = scene.abortListeners();

      await collectThroughWindow(canary);

      expect(
        wrapper.sentRedirects,
        'the adapter wrote redirect "manual" in one call: the arms differ only by what the wrapper forces',
      ).toStrictEqual(["manual"]);
      expect(
        listenersBeforeCollection,
        "before the collection the probe's signal carries two abort listeners: the runner's cancel and the runtime's own",
      ).toBe(2);
      expect(scene.abortListeners(), listenerRule).toBe(listenersAfterCollection);

      await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS);
      await settlesWithin(
        pendingProbe,
        PROBE_END_DEADLINE_MS,
        "the probe never ended after its clock: the body read still holds it",
      );

      expect(scene.vote, "the clock decides the probe once").toHaveBeenCalledTimes(1);
      expect(scene.vote, "an expired probe proves no recovery").toHaveBeenCalledWith(
        "FAILURE",
      );
      expect(
        scene.log.info,
        "the clock that decided writes one line, and no other line follows",
      ).toHaveBeenCalledTimes(1);
      expect(
        scene.log.info,
        "the head had arrived: the line says the upstream had not finished its reply",
      ).toHaveBeenCalledWith({
        event: "breaker_probe_clock_expired",
        req_id: "breaker_probe",
        upstream_reply: "not_finished",
      });
      expect(scene.log.error, "a clock expiry is not an error line").not.toHaveBeenCalled();
      await waitFor(
        () => scene.socketsClosed() === 1,
        SOCKET_CLOSE_DEADLINE_MS,
        "the cancel did not close the real upstream connection",
      );
    },
    15_000,
  );
});

// A non-2xx head whose body stalls: the adapter reads that body with its own
// reader, which the probe cannot cancel, so only the abort can end the read.
const errorBodyArms = [
  { ...arms[0], attemptAfterClock: "ends with one FAILURE vote" },
  { ...arms[1], attemptAfterClock: "stays pending without a vote" },
] as const;

describe("breaker probe — real runtime, a stalled error body after a garbage collection", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each(errorBodyArms)(
    'redirect "$option": at its clock the attempt $attemptAfterClock',
    async ({
      forceRedirect,
      listenersAfterCollection,
      listenerRule,
      attemptAfterClock,
    }) => {
      const wrapper = wrapGlobalFetch(forceRedirect);
      const scene = await startScene("error-body-stalled");
      const canary = newCanary();
      let ended = false;
      const pendingProbe = scene.start().finally(() => {
        ended = true;
      });

      try {
        await waitFor(() => scene.headsSent() === 1, HEAD_DEADLINE_MS, "the upstream never sent the probe's head");
        await sleep(KEEPALIVE_EVERY_MS);
        const listenersBeforeCollection = scene.abortListeners();

        await collectThroughWindow(canary);

        expect(
          wrapper.sentRedirects,
          'the adapter wrote redirect "manual" in one call: the arms differ only by what the wrapper forces',
        ).toStrictEqual(["manual"]);
        expect(
          listenersBeforeCollection,
          "before the collection the probe's signal carries two abort listeners: the runner's cancel and the runtime's own",
        ).toBe(2);
        expect(scene.abortListeners(), listenerRule).toBe(listenersAfterCollection);

        await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS);

        if (attemptAfterClock === "ends with one FAILURE vote") {
          await settlesWithin(
            pendingProbe,
            PROBE_END_DEADLINE_MS,
            "the probe never ended after its clock: the error body read still holds it",
          );
          expect(
            scene.vote.mock.calls,
            "an attempt that ends at its clock votes FAILURE once",
          ).toStrictEqual([["FAILURE"]]);
          expect(
            scene.log.info,
            "the clock that decided writes exactly one line",
          ).toHaveBeenCalledTimes(1);
          expect(
            scene.log.info,
            "the one line is the clock-expired event",
          ).toHaveBeenCalledWith(
            expect.objectContaining({ event: "breaker_probe_clock_expired" }),
          );
          await waitFor(
            () => scene.socketsClosed() === 1,
            SOCKET_CLOSE_DEADLINE_MS,
            "the abort did not close the upstream connection",
          );
        } else {
          await sleep(HELD_ATTEMPT_OBSERVATION_MS);
          expect(
            ended,
            "control arm: with the listener gone, the abort does not reach the error body read and the attempt stays pending through the window",
          ).toBe(false);
          expect(
            scene.vote.mock.calls,
            "control arm: a pending attempt has not voted",
          ).toStrictEqual([]);
          expect(
            scene.socketsClosed(),
            "control arm: the upstream connection stays open through the window",
          ).toBe(0);
        }
      } finally {
        // Closing the upstream ends a read the abort did not reach, so the
        // attempt finishes before the next cell starts.
        await scene.close();
        await settlesWithin(
          pendingProbe.catch(() => undefined),
          PROBE_END_DEADLINE_MS,
          "the probe did not end after its upstream closed",
        );
      }
    },
    15_000,
  );
});
