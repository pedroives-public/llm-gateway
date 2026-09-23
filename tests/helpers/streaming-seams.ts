import type { ProxyRouteOptions } from "../../src/routes/proxy.js";

// The forbidden pattern: an SSE body pushed through the buffered
// accumulate-then-parse can only end `undecodable` with breaker delta 1, so
// the gateway would open its own shared breaker on healthy traffic.
export const BUFFERED_READ_REACHED_BY_STREAM =
  "a stream: true body reached the buffered whole-body read; an SSE body there ends undecodable with breaker delta 1 and opens the shared breaker on healthy traffic";

export const STREAMING_SEAM_REACHED_WITHOUT_STREAM =
  "a request whose validated stream is not true reached the streaming seam";

// A measurement of the fetch body's cancel() on loopback saw the server
// observe the close within 300 ms (three runs). Three times that window and
// well under the runner's 5 s timeout, so a missing cancel fails with its own
// sentence instead of a runner timeout.
export const UPSTREAM_CLOSE_DEADLINE_MS = 1_000;

type BufferedSeam = ProxyRouteOptions["upstreamBuffered"];
type StreamingSeam = NonNullable<ProxyRouteOptions["upstreamStreaming"]>;

// Tripwire seams. A seam that throws does not fail the cell by itself: the
// route votes INCONCLUSIVE and rethrows, and its error handler answers an
// unrecognized throw with 500 gateway-fault, so a throw alone would surface
// as a different status. The sentence is also recorded, and each cell
// asserts the record first, so a body that reaches the wrong seam fails
// naming what went wrong.
export function bufferedTripwire(
  violations: string[],
  delegate: BufferedSeam = () =>
    Promise.resolve({ kind: "ok", status: 200, body_parsed: {} }),
): { seam: BufferedSeam; calls: () => number } {
  let calls = 0;
  return {
    calls: () => calls,
    seam: (body, signal, log) => {
      calls += 1;
      if (body.stream === true) {
        violations.push(BUFFERED_READ_REACHED_BY_STREAM);
        return Promise.reject(new Error(BUFFERED_READ_REACHED_BY_STREAM));
      }
      return delegate(body, signal, log);
    },
  };
}

export function streamingTripwire(violations: string[]): {
  seam: StreamingSeam;
  calls: () => number;
} {
  let calls = 0;
  return {
    calls: () => calls,
    seam: (body) => {
      calls += 1;
      if (body.stream !== true) {
        violations.push(STREAMING_SEAM_REACHED_WITHOUT_STREAM);
        return Promise.reject(
          new Error(STREAMING_SEAM_REACHED_WITHOUT_STREAM),
        );
      }
      // The terminal is not the subject of the cells that use this seam; any
      // error outcome ends the request.
      return Promise.resolve({ kind: "undecodable" });
    },
  };
}

// Races an observation that should happen against a deadline that rejects
// with its own sentence, so an absence fails named instead of timing out.
export async function withinDeadline<T>(
  promise: Promise<T>,
  ms: number,
  sentence: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(sentence));
    }, ms);
  });
  try {
    return await Promise.race([promise, deadline]);
  } finally {
    clearTimeout(timer);
  }
}
