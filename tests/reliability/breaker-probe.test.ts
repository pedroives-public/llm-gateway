import { describe, it, expect, vi } from "vitest";
import { createProbeRunner } from "../../src/reliability/breaker-probe.js";

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
});
