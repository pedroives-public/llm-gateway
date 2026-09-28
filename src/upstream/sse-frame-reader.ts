// Provisional upper bound BY CONSTRUCTION, not a measured frame size: no
// single SSE frame can exceed the byte total of the stream that carries it,
// and the largest stream measured so far totalled 445 KB (33k-token probe,
// 2026-09-09), rounded up to 500 KiB. Loose by orders of magnitude. Replace
// with the largest legitimate frame the provider can send (tool-call arguments
// chunk, final `usage` chunk, error event) once that probe exists.
//
// Not the buffered 1 MiB response cap: that cap fires on a response the parser
// would accept and that is merely large, which is no evidence against the
// upstream. This one fires when bytes never form a frame, the same evidence
// class as a 2xx that is not SSE.
export const PARSER_BUFFER_CAP = 500 * 1024;

export type Frame = {
  kind: "frame";
  bytes: Uint8Array;
  data: string | null;
};

export type End = {
  kind: "eof" | "eof_partial" | "cap";
};

export type SseFrame = Frame | End;

export function createSseFrameReader(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): () => Promise<SseFrame> {
  const decoder = new TextDecoder();
  let stored = new Uint8Array(0);
  let bodyClosed = false;

  return async function nextFrame(): Promise<SseFrame> {
    while (true) {
      const end = findFrameEnd(stored, bodyClosed);

      if (end !== "none") {
        const frame = stored.slice(0, end);
        stored = stored.slice(end);

        const text = decoder.decode(frame);
        const validData = text
          .split(/\r\n|\r|\n/)
          .filter((line) => line.startsWith("data:") || line === "data");

        if (validData.length === 0) {
          return { kind: "frame", bytes: frame, data: null };
        }

        const data = validData
          .map((line) => line.slice(5))
          .map((line) => (line.startsWith(" ") ? line.slice(1) : line))
          .join("\n");

        return { kind: "frame", bytes: frame, data: data };
      }

      if (bodyClosed) {
        if (stored.length > 0) {
          return { kind: "eof_partial" };
        }

        return { kind: "eof" };
      }

      const { value, done } = await reader.read();

      if (done) {
        bodyClosed = true;
        continue;
      }

      if (stored.length + value.length > PARSER_BUFFER_CAP) {
        return { kind: "cap" };
      }

      if (!value || value.length === 0) {
        continue;
      }

      const newStored = new Uint8Array(stored.length + value.length);
      newStored.set(stored, 0);
      newStored.set(value, stored.length);
      stored = newStored;
    }
  };
}

// A frame end is a position in the held bytes, or "none". The brand makes a
// bare number, such as a -1 meant as "not found", a compile error wherever a
// FrameEnd is expected: only the cast in findFrameEnd turns a number into a
// FrameEndIndex. That cast is not checked, at compile time or at runtime, so
// a wrong number passed through it is caught only by the reader's tests.
type FrameEndIndex = number & { readonly __brand: "FrameEndIndex" };
type FrameEnd = FrameEndIndex | "none";

function findFrameEnd(bytes: Uint8Array, bodyClosed: boolean): FrameEnd {
  let lineStart = 0;
  let i = 0;

  while (i < bytes.length) {
    const lineEndLength = lineEndLengthAt(bytes, i, bodyClosed);
    if (lineEndLength === 0) {
      i++;
      continue;
    }

    if (lineEndLength === "undecided") {
      return "none";
    }

    const lineEnd = i + lineEndLength;

    if (i === lineStart) {
      return lineEnd as FrameEndIndex;
    }

    i = lineEnd;
    lineStart = i;
  }

  return "none";
}

type LineEnd = 0 | 1 | 2 | "undecided";

function lineEndLengthAt(
  bytes: Uint8Array,
  index: number,
  bodyClosed: boolean,
): LineEnd {
  if (bytes[index] === 0x0a) {
    return 1;
  }

  if (bytes[index] === 0x0d) {
    if (index === bytes.length - 1) {
      if (bodyClosed) {
        return 1;
      }

      return "undecided";
    }

    if (index + 1 < bytes.length && bytes[index + 1] === 0x0a) {
      return 2;
    }

    return 1;
  }

  return 0;
}
