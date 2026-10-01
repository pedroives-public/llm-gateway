// Provisional bound by construction, not a measured frame size: no frame can
// exceed the stream that carries it, and the largest stream measured totalled
// 445 KB (2026-09-09), rounded up to 500 KiB. Replace it with the largest
// legitimate frame the provider sends (tool-call arguments, final `usage`
// chunk, error event) once that is measured.
//
// It bounds the bytes held without a frame after each chunk is searched, not
// the buffer's capacity: held bytes can reach the cap plus one chunk (64 KiB
// at most, measured locally), and doubling keeps the capacity under twice
// that: about 1.1 MiB per stream, briefly 1.65 MiB while it grows, about
// 68 MiB across ADMISSION_CAPACITY_POST_AUTH (41) streams.
//
// Unlike the buffered response cap, which fires on a large but valid response,
// this one fires on bytes that never form a frame: evidence against the
// upstream, like a 2xx that is not SSE.
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

type ScanCursor = {
  index: number;
  lineStart: number;
};

export function createSseFrameReader(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): () => Promise<SseFrame> {
  const decoder = new TextDecoder("utf-8", { ignoreBOM: true });
  let stored = new Uint8Array(1);
  let filled = 0;

  let bodyClosed = false;
  let leadingBOMChecked = false;
  let scanCursor: ScanCursor = { index: 0, lineStart: 0 };

  return async function nextFrame(): Promise<SseFrame> {
    while (true) {
      const search = findFrameEnd(
        stored.subarray(0, filled),
        scanCursor,
        bodyClosed,
      );
      scanCursor = search.cursor;

      if (search.end !== "none") {
        const frame = stored.slice(0, search.end);
        // The remainder is copied into an array of its own size, so positions
        // restart at zero and a buffer grown for a large frame is released
        // with it. When one chunk carries many frames this copy is most of the
        // cost (bare line ends: about 4 microseconds per frame, 0.4 without
        // it, measured 2026-10-01). Switch to a start offset over a retained
        // buffer, as openai-node's iterSSEChunks does, if a load probe shows
        // this copy in the CPU profile.
        stored = stored.slice(search.end, filled);
        filled -= search.end;

        const decoded = decoder.decode(frame);
        const text =
          !leadingBOMChecked && decoded.startsWith("\uFEFF")
            ? decoded.slice(1)
            : decoded;
        leadingBOMChecked = true;

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
        if (filled > 0) {
          return { kind: "eof_partial" };
        }

        return { kind: "eof" };
      }

      if (filled > PARSER_BUFFER_CAP) {
        return { kind: "cap" };
      }

      const { value, done } = await reader.read();

      if (done) {
        bodyClosed = true;
        continue;
      }

      if (filled + value.length > stored.length) {
        const newStored = new Uint8Array(
          Math.max(stored.length * 2, filled + value.length),
        );
        newStored.set(stored.subarray(0, filled));
        stored = newStored;
      }

      stored.set(value, filled);
      filled += value.length;
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

type FrameSearch = {
  end: FrameEnd;
  cursor: ScanCursor;
};

function findFrameEnd(
  bytes: Uint8Array,
  cursor: ScanCursor,
  bodyClosed: boolean,
): FrameSearch {
  let lineStart = cursor.lineStart;
  let index = cursor.index;

  while (index < bytes.length) {
    const lineEndLength = lineEndLengthAt(bytes, index, bodyClosed);
    if (lineEndLength === 0) {
      index++;
      continue;
    }

    if (lineEndLength === "undecided") {
      return { end: "none", cursor: { index, lineStart } };
    }

    const lineEnd = index + lineEndLength;

    if (index === lineStart) {
      return {
        end: lineEnd as FrameEndIndex,
        cursor: { index: 0, lineStart: 0 },
      };
    }

    index = lineEnd;
    lineStart = index;
  }

  return {
    end: "none",
    cursor: { index, lineStart },
  };
}

type LineEnd = 0 | 1 | 2 | "undecided";

const CR = 0x0d;
const LF = 0x0a;

function lineEndLengthAt(
  bytes: Uint8Array,
  index: number,
  bodyClosed: boolean,
): LineEnd {
  if (bytes[index] === LF) {
    return 1;
  }

  if (bytes[index] === CR) {
    if (index === bytes.length - 1) {
      if (bodyClosed) {
        return 1;
      }

      return "undecided";
    }

    if (index + 1 < bytes.length && bytes[index + 1] === LF) {
      return 2;
    }

    return 1;
  }

  return 0;
}
