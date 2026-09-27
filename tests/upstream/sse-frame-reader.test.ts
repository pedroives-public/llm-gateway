import { describe, it, expect } from "vitest";
import {
  PARSER_BUFFER_CAP,
  createSseFrameReader,
} from "../../src/upstream/sse-frame-reader.js";
import { withinDeadline } from "../helpers/streaming-seams.js";

// For a body that stays open, the cells whose answer is decided from bytes
// already in memory (the cap, a lone CR followed by another byte): nothing the
// decision needs is still in flight. One second is far above that and well
// under the runner's 5 s timeout, so a reader that keeps waiting for more
// bytes fails with its own sentence instead of a runner timeout.
const IN_MEMORY_DECISION_DEADLINE_MS = 1_000;

const encode = (text: string): Uint8Array => new TextEncoder().encode(text);

// A body that delivers these chunks, one per read, then closes.
function closedBody(
  chunks: readonly Uint8Array[],
): ReadableStreamDefaultReader<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      chunks.forEach((chunk) => {
        controller.enqueue(chunk);
      });
      controller.close();
    },
  }).getReader();
}

function frame(text: string, data: string | null) {
  return { kind: "frame", bytes: encode(text), data };
}

// Pulls exactly `count` results, in order. Every row ends on an end, so no
// pull ever follows one.
async function pullTimes<T>(
  nextFrame: () => Promise<T>,
  count: number,
): Promise<readonly T[]> {
  if (count === 0) {
    return [];
  }
  const head = await nextFrame();
  return [head, ...(await pullTimes(nextFrame, count - 1))];
}

describe("createSseFrameReader", () => {
  it("a frame split across two chunks comes out whole, with its original bytes", async () => {
    const reader = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: hel"));
        controller.enqueue(new TextEncoder().encode("lo\n\n"));
        controller.close();
      },
    }).getReader();

    const nextFrame = createSseFrameReader(reader);

    expect(
      await nextFrame(),
      "a frame ends only at an empty line (WHATWG HTML, 'Interpreting an event stream'), so a frame split across chunks is held until that line arrives and comes out whole",
    ).toStrictEqual({
      kind: "frame",
      bytes: new TextEncoder().encode("data: hello\n\n"),
      data: "hello",
    });
    expect(
      await nextFrame(),
      "the body closed exactly at a frame boundary, so the end is a clean eof, not eof_partial",
    ).toStrictEqual({
      kind: "eof",
    });
  });

  // Each row feeds a body that closes and pulls exactly as many results as it
  // expects; every expected result is written out by hand.
  it.each([
    {
      name: "several frames in one chunk",
      chunks: [encode("data: a\n\ndata: b\n\n")],
      expected: [
        frame("data: a\n\n", "a"),
        frame("data: b\n\n", "b"),
        { kind: "eof" },
      ],
      because:
        "a chunk and a frame are independent units: one chunk may carry several frames, and each comes out alone with its own bytes",
    },
    {
      name: "a comment, an id and an unknown field around the data",
      chunks: [encode(": keep-alive\nid: 7\nfoo: bar\ndata: x\n\n")],
      expected: [
        frame(": keep-alive\nid: 7\nfoo: bar\ndata: x\n\n", "x"),
        { kind: "eof" },
      ],
      because:
        "the reader observes and never rewrites: every line reaches the forwarded bytes intact, and only the data field feeds the text",
    },
    {
      name: "a frame with no data field",
      chunks: [encode(": ping\n\n")],
      expected: [frame(": ping\n\n", null), { kind: "eof" }],
      because:
        "a frame without a data field yields no text, which is distinct from an empty text",
    },
    {
      name: "several data lines in one frame",
      chunks: [encode("data: a\ndata: b\n\n")],
      expected: [frame("data: a\ndata: b\n\n", "a\nb"), { kind: "eof" }],
      because:
        "the values of all data lines are joined by a line feed (WHATWG HTML, 'Interpreting an event stream')",
    },
    {
      name: "one leading space after the colon",
      chunks: [encode("data:x\n\ndata:  y\n\n")],
      expected: [
        frame("data:x\n\n", "x"),
        frame("data:  y\n\n", " y"),
        { kind: "eof" },
      ],
      because:
        "one leading space after the colon is removed, and only one (WHATWG HTML, 'Interpreting an event stream')",
    },
    {
      name: "CRLF line endings",
      chunks: [encode("data: a\r\n\r\n")],
      expected: [frame("data: a\r\n\r\n", "a"), { kind: "eof" }],
      because:
        "a line may end in CRLF (WHATWG HTML, 'Interpreting an event stream'); a reader that knew only LF would never form this frame and would blame a correct upstream through PARSER_BUFFER_CAP",
    },
    {
      name: "lone CR line endings",
      chunks: [encode("data: a\r\r")],
      expected: [frame("data: a\r\r", "a"), { kind: "eof" }],
      because:
        "a line may end in a lone CR (WHATWG HTML, 'Interpreting an event stream')",
    },
    {
      name: "a CR that ends a chunk and an LF that starts the next",
      chunks: [encode("data: a\r"), encode("\ndata: b\r\n\r\n")],
      expected: [frame("data: a\r\ndata: b\r\n\r\n", "a\nb"), { kind: "eof" }],
      because:
        "a CR that ends a chunk is held until the next byte shows whether it begins a CRLF; ending the line at the CR would read the LF as an empty line and cut the frame in two",
    },
    {
      name: "a CR that ends a chunk and closes the empty line",
      chunks: [encode("data: a\r\n\r"), encode("\ndata: b\r\n\r\n")],
      expected: [
        frame("data: a\r\n\r\n", "a"),
        frame("data: b\r\n\r\n", "b"),
        { kind: "eof" },
      ],
      because:
        "a CR held at the end of a chunk may be the first byte of the CRLF that ends the frame: deciding it early cuts the frame before its LF and leaves that LF behind as an empty frame",
    },
    {
      name: "a CRLF that the closing must not split",
      chunks: [encode("data: a\r\nb")],
      expected: [{ kind: "eof_partial" }],
      because:
        "the closing decides only a CR that is the last byte held; a CR followed by LF stays one CRLF, so the line b never ends, no empty line exists and the end is eof_partial",
    },
    {
      name: "a CRLF line followed by a final CR when the body closes",
      chunks: [encode("data: a\r\n\r")],
      expected: [frame("data: a\r\n\r", "a"), { kind: "eof" }],
      because:
        "the closing turns a final held CR into a line end, but a CR followed by LF is still one CRLF: the frame keeps all its bytes and no empty frame is left over",
    },
    {
      name: "a data field with an empty value",
      chunks: [encode("data:\n\n")],
      expected: [frame("data:\n\n", ""), { kind: "eof" }],
      because:
        "an empty data value yields the empty text, not no text: the field appends a line feed to the data buffer and dispatch removes it (WHATWG HTML, 'Interpreting an event stream'), and the official OpenAI SDK yields the same empty text",
    },
    {
      name: "a byte-order mark at the start of the body",
      chunks: [encode("﻿data: x\n\n")],
      expected: [frame("﻿data: x\n\n", "x"), { kind: "eof" }],
      because:
        "one leading byte-order mark is stripped from the text (WHATWG HTML, 'Interpreting an event stream') and kept in the forwarded bytes",
    },
    {
      name: "a two-byte character split across chunks",
      chunks: [
        Uint8Array.of(...encode("data: ol"), 0xc3),
        Uint8Array.of(0xa1, ...encode("\n\n")),
      ],
      expected: [frame("data: olá\n\n", "olá"), { kind: "eof" }],
      because:
        "text is decoded across chunk boundaries: a character whose bytes arrive in two chunks is one character, never two replacement characters",
    },
    {
      name: "the upstream's terminating frame",
      chunks: [encode("data: [DONE]\n\n")],
      expected: [frame("data: [DONE]\n\n", "[DONE]"), { kind: "eof" }],
      because:
        "the reader gives [DONE] no meaning: it yields an ordinary frame, and the arbiter is the one that compares the text with [DONE]",
    },
    {
      name: "an empty body",
      chunks: [],
      expected: [{ kind: "eof" }],
      because:
        "a body that closes with no bytes closes at a frame boundary: the end is eof, and what an empty stream means is the caller's decision",
    },
    {
      name: "a body that closes inside a frame",
      chunks: [encode("data: a\n\ndata: b\n")],
      expected: [frame("data: a\n\n", "a"), { kind: "eof_partial" }],
      because:
        "bytes without their empty line never form a frame: the end is eof_partial, and the incomplete bytes are never yielded",
    },
  ])("$name", async ({ chunks, expected, because }) => {
    const nextFrame = createSseFrameReader(closedBody(chunks));

    expect(await pullTimes(nextFrame, expected.length), because).toStrictEqual(
      expected,
    );
  });

  it("bytes that pass PARSER_BUFFER_CAP without an empty line end in cap while the body is still open", async () => {
    // The body never closes: only the cap can end this read.
    const reader = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encode(`data: ${"x".repeat(PARSER_BUFFER_CAP)}`));
      },
    }).getReader();
    const nextFrame = createSseFrameReader(reader);

    expect(
      await withinDeadline(
        nextFrame(),
        IN_MEMORY_DECISION_DEADLINE_MS,
        "PARSER_BUFFER_CAP must end a read whose incomplete frame passed it: no result came before the deadline",
      ),
      "PARSER_BUFFER_CAP bounds the bytes held without a frame: once they pass it, the end is cap, the same evidence class as a 2xx that is not SSE",
    ).toStrictEqual({ kind: "cap" });
  });

  it("a lone CR followed by another byte ends its line while the body is still open", async () => {
    // The body never closes, so the closing cannot turn the CRs into line
    // ends: only reading the byte after each CR can.
    const reader = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encode("data: a\r\rdata: b"));
      },
    }).getReader();
    const nextFrame = createSseFrameReader(reader);

    expect(
      await withinDeadline(
        nextFrame(),
        IN_MEMORY_DECISION_DEADLINE_MS,
        "a lone CR followed by another byte must end its line while the body is open: no frame came before the deadline",
      ),
      "a CR followed by a byte other than LF is a lone CR, a complete line end (WHATWG HTML, 'Interpreting an event stream'); only a CR that is the last byte held waits for the next one",
    ).toStrictEqual(frame("data: a\r\r", "a"));
  });

  it("PARSER_BUFFER_CAP bounds one incomplete frame, not the bytes of the whole body", async () => {
    // Three frames, one per chunk: their total passes the cap, and each one
    // stays under it.
    const text = `data: ${"x".repeat(Math.floor(PARSER_BUFFER_CAP / 2))}\n\n`;
    const nextFrame = createSseFrameReader(
      closedBody([encode(text), encode(text), encode(text)]),
    );

    // Only the kinds: the bytes and text of a frame are pinned by the rows
    // above, and a failing diff of three half-cap frames would bury the kind.
    const kinds = (await pullTimes(nextFrame, 4)).map((result) => result.kind);
    expect(
      kinds,
      "the cap bounds the bytes held in one incomplete frame; a body of many frames may total any size",
    ).toStrictEqual(["frame", "frame", "frame", "eof"]);
  });

  it("a transport failure while a frame is half read is a rejection, not an end", async () => {
    const failure = new Error("socket reset");
    // start() queues half a frame; pull() runs only once that chunk has been
    // read, and fails the body.
    const reader = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encode("data: b"));
      },
      pull(controller) {
        controller.error(failure);
      },
    }).getReader();
    const nextFrame = createSseFrameReader(reader);

    // Settled by hand: expect().rejects drops the message when the promise
    // resolves instead, and resolving to an end is the defect this cell names.
    const settled = await nextFrame().then(
      (value: unknown) => ({ outcome: "resolved", value }),
      (error: unknown) => ({ outcome: "rejected", same: error === failure }),
    );
    expect(
      settled,
      "a transport failure stays a rejection carrying its own error, which the caller classifies as it does the buffered read's; it is never turned into an end such as eof_partial",
    ).toStrictEqual({ outcome: "rejected", same: true });
  });
});
