// Provisional upper bound BY CONSTRUCTION, not a measured frame size: no
// single SSE frame can exceed the byte total of the stream that carries it,
// and the largest stream measured so far totalled 445 KB (33k-token probe,
// 2026-09-09), rounded up to 500 KiB. Loose by orders of magnitude. Replace
// with the largest legitimate frame the provider can send (tool-call arguments
// chunk, final `usage` chunk, error event) once that probe exists.
//
// What it bounds: the bytes still held without a frame after the newest chunk
// has been searched, never the capacity of the buffer that holds them. The
// check runs just before the next read, so held bytes can briefly reach the cap
// plus one chunk (largest measured locally: 64 KiB; Node 22.22.3, undici
// 6.24.1). The buffer doubles when a chunk does not fit, so one stream holds up
// to 1 MiB, and 1.5 MiB for an instant while it is copied into a larger one:
// about 62 MiB across ADMISSION_CAPACITY_POST_AUTH (41) streams.
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
        // The remainder is copied into an array of its own size: positions keep
        // starting at zero, and a buffer grown for a large frame is released
        // with the frame. The copy costs the size of the remainder once per
        // frame, which only shows when one chunk carries many frames (a 64 KiB
        // chunk of bare line ends took about 0.3 s, measured 2026-09-30, mostly
        // the fixed cost per frame). Move to a start offset over a retained
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
