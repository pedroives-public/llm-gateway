import http from "node:http";
import v8 from "node:v8";
import vm from "node:vm";
import { getEventListeners } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PROBE_CLOCK_MS } from "../../src/config.js";
import { createProbeRunner } from "../../src/reliability/breaker-probe.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";

// A real garbage collection, without the --expose-gc flag on the runner.
v8.setFlagsFromString("--expose-gc");
const collectGarbage = vm.runInNewContext("gc") as () => void;

// Captured before the cell fakes setTimeout, so waits run on real time.
const realSetTimeout = globalThis.setTimeout;
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => realSetTimeout(resolve, ms));

// Keepalive cadence of the local upstream below (real time).
const KEEPALIVE_EVERY_MS = 200;
// Real-time bounds for the waits of the scene; each fails with its own
// sentence, under Vitest's test timeout set on the cell.
const HEAD_DEADLINE_MS = 2_000;
const COLLECTION_DEADLINE_MS = 2_000;
const PROBE_END_DEADLINE_MS = 2_000;
const SOCKET_CLOSE_DEADLINE_MS = 2_000;
const POLL_STEP_MS = 20;

// Rejects with `sentence` when `promise` has not settled within `ms` of real
// time; used where the defect is an absence (a probe that never ends).
function settlesWithin<T>(promise: Promise<T>, ms: number, sentence: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => {
      realSetTimeout(() => reject(new Error(sentence)), ms);
    }),
  ]);
}

async function waitFor(check: () => boolean, ms: number, sentence: string): Promise<void> {
  const startedAt = performance.now();
  while (!check()) {
    if (performance.now() - startedAt > ms) throw new Error(sentence);
    await sleep(POLL_STEP_MS);
  }
}

// The real adapter, the real runner and a local upstream that sends a 2xx SSE
// head and then, depending on `mode`, keepalive comments or nothing; never
// [DONE]. Only setTimeout and
// clearTimeout are faked, so the probe's clock moves on demand while sockets,
// the upstream's keepalives and the garbage collector stay real.
async function startScene(mode: "keepalive" | "stalled") {
  let headsSent = 0;
  let socketsClosed = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write(": open\n\n");
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
    abortListeners: () => (probeSignal === undefined ? -1 : getEventListeners(probeSignal, "abort").length),
    close: async () => {
      vi.useRealTimers();
      server.closeAllConnections();
      await upstream.close();
    },
  };
}

describe("breaker probe — real runtime, after a garbage collection", () => {
  let close: (() => Promise<void>) | undefined;

  afterEach(async () => {
    await close?.();
    close = undefined;
  });

  // With keepalives, a chunk after the clock also wakes the probe's read loop;
  // a stalled body sends nothing, so only the cancel can end the read.
  it.each(["keepalive", "stalled"] as const)(
    "ends at its clock even though the runtime lost the abort (%s)",
    async (mode) => {
      const scene = await startScene(mode);
      close = scene.close;

      const pendingProbe = scene.start();
      await waitFor(() => scene.headsSent() === 1, HEAD_DEADLINE_MS, "the upstream never sent the probe's head");
      await sleep(KEEPALIVE_EVERY_MS);
      const listenersBeforeCollection = scene.abortListeners();

      collectGarbage();
      await waitFor(
        () => {
          collectGarbage();
          return scene.abortListeners() < listenersBeforeCollection;
        },
        COLLECTION_DEADLINE_MS,
        "no collection removed the runtime's abort listener: the scene no longer reproduces the lost abort",
      );
      const listenersAfterCollection = scene.abortListeners();

      expect(
        listenersBeforeCollection,
        "before the collection the probe's signal carries two abort listeners: the runner's cancel and the runtime's own",
      ).toBe(2);
      expect(
        listenersAfterCollection,
        "control: the collection removed the runtime's abort listener, so an abort can no longer reach the read",
      ).toBe(1);

      await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS);
      await settlesWithin(
        pendingProbe,
        PROBE_END_DEADLINE_MS,
        "the probe never ended after its clock: the lost abort still holds it",
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
