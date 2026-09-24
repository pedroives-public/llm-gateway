import { describe, it, expect } from "vitest";
import { cancelStream } from "../../src/upstream/stream.js";

// cancelStream closes a body the gateway will not read. When the reader's
// cancel() rejects, the body is already closed, so the rejection becomes one
// upstream_cancel_rejected line and never a throw. The rejection is produced
// by a real ReadableStream whose underlying source rejects its cancel: the
// stream still closes, and only the caller of cancel() learns of the failure.

class AbortLikeError extends Error {
  override name = "AbortError";
}

function readerWhoseCancel(
  outcome: "resolves" | { rejectWith: unknown },
): ReadableStreamDefaultReader<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    cancel() {
      return outcome === "resolves"
        ? undefined
        : Promise.reject(outcome.rejectWith);
    },
  }).getReader();
}

describe("cancelStream: a rejected cancel is one allowlisted log line", () => {
  it.each([
    {
      case: "cancel() resolves",
      site: "proxy_route",
      reader: () => readerWhoseCancel("resolves"),
      expected: [],
    },
    {
      case: "cancel() rejects with a named error that carries a message and a cause",
      site: "proxy_route",
      reader: () =>
        readerWhoseCancel({
          rejectWith: new AbortLikeError("SECRET-IN-MESSAGE", {
            cause: { name: "CauseName" },
          }),
        }),
      expected: [
        {
          event: "upstream_cancel_rejected",
          site: "proxy_route",
          err_name: "AbortError",
        },
      ],
    },
    {
      case: "cancel() rejects with a value that has no name",
      site: "upstream_adapter",
      reader: () => readerWhoseCancel({ rejectWith: "boom" }),
      expected: [
        {
          event: "upstream_cancel_rejected",
          site: "upstream_adapter",
          err_name: "UNKNOWN",
        },
      ],
    },
  ] as const)("$case", async ({ site, reader, expected }) => {
    const lines: object[] = [];
    const log = {
      info: (line: object) => {
        lines.push(line);
      },
    };

    const settled = await cancelStream(reader(), log, site).then(
      () => "resolved",
      (error: unknown) => `rejected: ${String(error)}`,
    );

    expect(
      settled,
      "cancelStream must never throw: a rejected cancel is a cleanup observation, not a terminal",
    ).toBe("resolved");
    expect(
      lines,
      "a rejected cancel emits exactly one line, carrying only event, site and err_name: err_name is the error's own name, never its cause's, and the error message never reaches it",
    ).toStrictEqual(expected);
  });
});
