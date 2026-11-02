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

    await runProbe(vote);

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

    await runProbe(vi.fn());

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

    await runProbe(vote);

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

    await runProbe(vote);

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

    await runProbe(vote);

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

    await runProbe(vote);

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

  it("votes FAILURE at 3s when the adapter waits for abort", async () => {
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
    const runProbe = createProbeRunner({
      streaming: silent,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    const pendingProbe = runProbe(vote);

    await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS - 1);
    expect(vote, "the clock has not expired yet").not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(vote, "the deadline must decide the probe").toHaveBeenCalledTimes(1);
    expect(vote, "an expired clock proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );

    await pendingProbe;
  });

  it("votes FAILURE at 3s when the response body stalls", async () => {
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
    const runProbe = createProbeRunner({
      streaming: stalled,
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    });

    const pendingProbe = runProbe(vote);

    await vi.advanceTimersByTimeAsync(PROBE_CLOCK_MS);
    expect(vote, "a stalled body must not hold HALF_OPEN").toHaveBeenCalledTimes(
      1,
    );
    expect(vote, "an expired clock proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );

    await pendingProbe;
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

    const pendingProbe = runProbe(vote);
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
