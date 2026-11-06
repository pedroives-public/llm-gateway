import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { PROBE_CLOCK_MS } from "../../src/config.js";
import { createProbeRunner } from "../../src/reliability/breaker-probe.js";
import { PARSER_BUFFER_CAP } from "../../src/upstream/sse-frame-reader.js";
import type { StreamingUpstream } from "../../src/upstream/openai.js";
import type { StreamingAdapter } from "../../src/upstream/stream.js";

// The app is not closing in these scenes: a signal that never aborts.
const NOT_CLOSING = new AbortController().signal;

function readerOf(...chunks: string[]) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(new TextEncoder().encode(chunk));
      }
      controller.close();
    },
  }).getReader();
}

describe("breaker probe", () => {
  it("votes SUCCESS when the stream reaches [DONE]", async () => {
    const vote = vi.fn();
    const streaming = vi.fn(async () => ({
      kind: "accepted_stream" as const,
      status: 200,
      reader: readerOf("data: {}\n\n", "data: [DONE]\n\n"),
    }));
    const runProbe = createProbeRunner({
      streaming,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    await runProbe(vote, NOT_CLOSING);

    expect(vote, "a probe votes exactly once").toHaveBeenCalledTimes(1);
    expect(
      vote,
      "[DONE] inside the clock is the only evidence of recovery",
    ).toHaveBeenCalledWith("SUCCESS");
  });

  it("sends the fixed probe body using the configured model", async () => {
    const streaming = vi.fn(async () => ({
      kind: "accepted_stream" as const,
      status: 200,
      reader: readerOf("data: [DONE]\n\n"),
    }));
    const model = "configured-probe-model";
    const log = { info: () => {}, error: () => {} };
    const runProbe = createProbeRunner({ streaming, model, log });

    await runProbe(vi.fn(), NOT_CLOSING);

    expect(streaming, "a probe has no retry").toHaveBeenCalledTimes(1);
    expect(
      streaming,
      "the probe's request is the measured shape and carries only deployment config",
    ).toHaveBeenCalledWith(
      {
        model: "configured-probe-model",
        messages: [{ role: "user", content: "ping" }],
        stream: true,
        max_tokens: 1,
      },
      expect.any(AbortSignal),
      log,
    );
  });

  it.each([
    {
      scene: "stream ends without [DONE]",
      chunk: "data: {}\n\n",
      because: "a stream that ends without [DONE] proves no recovery",
    },
    {
      scene: "model content contains [DONE]",
      chunk: 'data: {"choices":[{"delta":{"content":"[DONE]"}}]}\n\n',
      because: "only the protocol's [DONE] counts, never the model's text",
    },
  ])("votes FAILURE once when $scene", async ({ chunk, because }) => {
    const vote = vi.fn();
    const streaming = vi.fn(async () => ({
      kind: "accepted_stream" as const,
      status: 200,
      reader: readerOf(chunk),
    }));
    const runProbe = createProbeRunner({
      streaming,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    await runProbe(vote, NOT_CLOSING);

    expect(vote, "a probe votes exactly once").toHaveBeenCalledTimes(1);
    expect(vote, because).toHaveBeenCalledWith("FAILURE");
  });

  it("votes FAILURE once when the adapter returns upstream_error", async () => {
    const vote = vi.fn();
    const streaming = vi.fn(async () => ({
      kind: "upstream_error" as const,
      status: 503,
      body_raw: "upstream unavailable",
    }));
    const runProbe = createProbeRunner({
      streaming,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    await runProbe(vote, NOT_CLOSING);

    expect(vote, "a probe votes exactly once").toHaveBeenCalledTimes(1);
    expect(
      vote,
      "an upstream error proves no recovery",
    ).toHaveBeenCalledWith("FAILURE");
  });

  it("votes FAILURE once when the adapter rejects", async () => {
    const vote = vi.fn();
    const streaming = vi.fn(async () => {
      throw new Error("boom");
    });
    const runProbe = createProbeRunner({
      streaming,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    await runProbe(vote, NOT_CLOSING);

    expect(
      vote,
      "an exception must still vote: a probe with no vote leaves HALF_OPEN with nothing armed",
    ).toHaveBeenCalledTimes(1);
    expect(vote, "an exception proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );
  });

  it("cancels a stream still open when it votes FAILURE", async () => {
    const vote = vi.fn();
    // Bytes that never form a frame, on a body that never closes: the frame
    // reader gives up at its cap while the upstream socket is still open.
    const reader = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(PARSER_BUFFER_CAP + 1).fill(97));
      },
    }).getReader();
    const cancel = vi.spyOn(reader, "cancel");
    const streaming = vi.fn(async () => ({
      kind: "accepted_stream" as const,
      status: 200,
      reader,
    }));
    const runProbe = createProbeRunner({
      streaming,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    await runProbe(vote, NOT_CLOSING);

    expect(vote, "a capped stream proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );
    expect(
      cancel,
      "a probe that fails with the stream open must release its socket",
    ).toHaveBeenCalledTimes(1);
  });
});

describe("breaker probe — wall clock", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("votes FAILURE at 10s when the adapter waits for abort", async () => {
    const vote = vi.fn();
    const silent = vi.fn(
      (...args: Parameters<StreamingUpstream>) =>
        new Promise<StreamingAdapter>((resolve) => {
          args[1].addEventListener(
            "abort",
            () =>
              resolve({
                kind: "aborted",
                abort_kind: "wall_clock_expired",
              }),
            { once: true },
          );
        }),
    );

    const log = { info: vi.fn(), error: vi.fn() };

    const runProbe = createProbeRunner({
      streaming: silent,
      model: "probe-model",
      log,
    });

    const pendingProbe = runProbe(vote, NOT_CLOSING);

    await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS - 1);
    expect(vote, "the clock has not expired yet").not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(vote, "the deadline must decide the probe").toHaveBeenCalledTimes(1);
    expect(vote, "an expired clock proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );

    await pendingProbe;

    expect(
      log.info,
      "the clock that decided writes one line, and no other line follows",
    ).toHaveBeenCalledTimes(1);
    expect(log.info, "before the response head the line says the upstream had not started its reply").toHaveBeenCalledWith({
      event: "breaker_probe_clock_expired",
      req_id: "breaker_probe",
      upstream_reply: "not_started",
    });
    expect(log.error, "a clock expiry is not an error line").not.toHaveBeenCalled();
  });

  it("votes FAILURE at 10s when the response body stalls", async () => {
    const vote = vi.fn();
    const stalled = vi.fn(async (...args: Parameters<StreamingUpstream>) => ({
      kind: "accepted_stream" as const,
      status: 200,
      reader: new ReadableStream<Uint8Array>({
        start(controller) {
          args[1].addEventListener(
            "abort",
            () => controller.error(args[1].reason),
            { once: true },
          );
        },
      }).getReader(),
    }));

    const log = { info: vi.fn(), error: vi.fn() };
    const runProbe = createProbeRunner({
      streaming: stalled,
      model: "probe-model",
      log,
    });

    const pendingProbe = runProbe(vote, NOT_CLOSING);

    await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS);
    expect(vote, "a stalled body must not hold HALF_OPEN").toHaveBeenCalledTimes(
      1,
    );
    expect(vote, "an expired clock proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );

    await pendingProbe;

    expect(
      log.info,
      "the clock that decided writes one line, and no other line follows",
    ).toHaveBeenCalledTimes(1);
    expect(log.info, "after the response head the line says the upstream had not finished its reply").toHaveBeenCalledWith({
      event: "breaker_probe_clock_expired",
      req_id: "breaker_probe",
      upstream_reply: "not_finished",
    });
    expect(log.error, "a clock expiry is not an error line").not.toHaveBeenCalled();
  });

  it("clears the clock, votes, then cancels the reader after [DONE]", async () => {
    const order: string[] = [];
    const timerCountsAtVote: number[] = [];
    const vote = vi.fn(() => {
      order.push("vote");
      timerCountsAtVote.push(vi.getTimerCount());
    });
    const reader = readerOf("data: [DONE]\n\n");
    let releaseCancel = (): void => {};
    const slowCancel = new Promise<void>((resolve) => {
      releaseCancel = resolve;
    });
    const cancel = vi.spyOn(reader, "cancel").mockImplementation(async () => {
      order.push("cancel");
      await slowCancel;
    });
    const streaming = vi.fn(async () => ({
      kind: "accepted_stream" as const,
      status: 200,
      reader,
    }));
    const runProbe = createProbeRunner({
      streaming,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    const pendingProbe = runProbe(vote, NOT_CLOSING);
    try {
      await vi.advanceTimersByTimeAsync(0);

      expect(vote, "a slow cleanup must not hold the vote").toHaveBeenCalledTimes(
        1,
      );
      expect(vote).toHaveBeenCalledWith("SUCCESS");
      expect(
        cancel,
        "[DONE] must release the socket: nothing else closes it",
      ).toHaveBeenCalledTimes(1);
      expect(
        timerCountsAtVote,
        "the clock must stop before the vote: no abort may reach a decided probe",
      ).toEqual([0]);
      expect(vi.getTimerCount(), "no clock may stay armed").toBe(0);
      expect(order, "the vote must not wait for the cleanup").toEqual([
        "vote",
        "cancel",
      ]);
    } finally {
      releaseCancel();
    }

    await pendingProbe;
  });
});

// Captured before any cell installs fake timers, so a deadline in real time
// can bound a promise that must settle.
const realSetTimeout = globalThis.setTimeout;

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

// Real-time bound for a probe that must have ended once its clock fired;
// under Vitest's 5 s test timeout so the failure carries its own sentence.
const PROBE_END_DEADLINE_MS = 2_000;
// Keepalive cadence of the body below: one SSE comment per second, the
// cadence of the upstream that held the probe in the review of 2026-11-06.
const KEEPALIVE_EVERY_MS = 1_000;

// After a garbage collection, with redirect "error", the runtime's fetch no
// longer delivers the abort to a body read in progress (measured 2026-11-06):
// the read neither ends nor errors. This body stands for that runtime: it
// never reacts to the abort; only a cancel of its reader ends it.
function bodyIgnoringAbort(mode: "stalled" | "keepalive") {
  let cancels = 0;
  let enqueue: ((text: string) => void) | undefined;
  let keepalive: ReturnType<typeof setInterval> | undefined;
  const reader = new ReadableStream<Uint8Array>({
    start(controller) {
      enqueue = (text) => controller.enqueue(new TextEncoder().encode(text));
      enqueue(": open\n\n");
      if (mode === "keepalive") {
        keepalive = setInterval(() => enqueue?.(": keepalive\n\n"), KEEPALIVE_EVERY_MS);
      }
    },
    cancel() {
      cancels += 1;
      clearInterval(keepalive);
      enqueue = undefined;
    },
  }).getReader();
  return {
    reader,
    cancels: () => cancels,
    // A [DONE] the upstream sends late; it reaches the probe only if the
    // reader was never cancelled.
    sendLateDone: () => enqueue?.("data: [DONE]\n\n"),
  };
}

describe("breaker probe — the clock decides when the abort never reaches the read", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each(["stalled", "keepalive"] as const)(
    "votes FAILURE at the clock and cancels the body (%s)",
    async (mode) => {
      const vote = vi.fn();
      const log = { info: vi.fn(), error: vi.fn() };
      const body = bodyIgnoringAbort(mode);
      const runProbe = createProbeRunner({
        streaming: vi.fn(async () => ({
          kind: "accepted_stream" as const,
          status: 200,
          reader: body.reader,
        })),
        model: "probe-model",
        log,
      });

      const pendingProbe = runProbe(vote, NOT_CLOSING);

      await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS - 1);

      expect(vote, "the probe clock has not expired").not.toHaveBeenCalled();
      expect(body.cancels(), "the body stays open before the deadline").toBe(0);

      await vi.advanceTimersByTimeAsync(1);

      expect(vote, "the clock decides the probe once").toHaveBeenCalledTimes(1);
      expect(vote, "an expired probe proves no recovery").toHaveBeenCalledWith(
        "FAILURE",
      );
      expect(body.cancels(), "the clock cancels the body").toBe(1);

      body.sendLateDone();

      await settlesWithin(
        pendingProbe,
        PROBE_END_DEADLINE_MS,
        "the probe did not settle after its clock expired",
      );

      expect(vote, "a late [DONE] cannot change the vote").toHaveBeenCalledTimes(1);

      expect(
        log.info,
        "the clock that decided writes one line, and no other line follows",
      ).toHaveBeenCalledTimes(1);
      expect(log.info, "after the response head the line says the upstream had not finished its reply").toHaveBeenCalledWith({
        event: "breaker_probe_clock_expired",
        req_id: "breaker_probe",
        upstream_reply: "not_finished",
      });
      expect(log.error, "a clock expiry is not an error line").not.toHaveBeenCalled();
    },
  );

  it("cancels a body whose head arrives as the clock expires", async () => {
    const vote = vi.fn();
    const log = { info: vi.fn(), error: vi.fn() };
    const body = bodyIgnoringAbort("stalled");
    // The adapter may hand back an accepted stream after the abort: its head
    // was processed in the same turn the clock fired.
    const headAtTheClock = vi.fn(
      (...args: Parameters<StreamingUpstream>) =>
        new Promise<StreamingAdapter>((resolve) => {
          args[1].addEventListener(
            "abort",
            () =>
              resolve({
                kind: "accepted_stream",
                status: 200,
                reader: body.reader,
              }),
            { once: true },
          );
        }),
    );
    const runProbe = createProbeRunner({
      streaming: headAtTheClock,
      model: "probe-model",
      log,
    });

    const pendingProbe = runProbe(vote, NOT_CLOSING);
    await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS);

    expect(vote, "the clock decides the probe once").toHaveBeenCalledTimes(1);
    expect(vote, "an expired probe proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );
    expect(body.cancels(), "the clock cancels the body").toBe(1);

    await settlesWithin(
      pendingProbe,
      PROBE_END_DEADLINE_MS,
      "the probe did not settle after its clock expired",
    );

    expect(
      log.info,
      "the clock that decided writes one line, and no other line follows",
    ).toHaveBeenCalledTimes(1);
    expect(
      log.info,
      "the line reports what the probe knew when the clock decided: no head yet",
    ).toHaveBeenCalledWith({
      event: "breaker_probe_clock_expired",
      req_id: "breaker_probe",
      upstream_reply: "not_started",
    });
    expect(log.error, "a clock expiry is not an error line").not.toHaveBeenCalled();
  });
});
