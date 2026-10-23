import { describe, it, expect, vi } from "vitest";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { AcceptedStream } from "../../src/upstream/stream.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { bufferedTripwire } from "../helpers/streaming-seams.js";
import { makeLogCapture } from "../log-capture.js";

// After the first frame, only the gateway's own abort can make the frame read
// reject with the signal's reason, and no gateway code produces the reasons
// below on a streaming request. The test holds a second controller whose
// signal is composed with the real helper's, so it can abort the request
// signal with such a reason while the real deadline and capabilities stay in
// place. The mock is file-wide, so these rows live in their own file.
const held = vi.hoisted(() => {
  const state: { controller: AbortController | undefined } = {
    controller: undefined,
  };
  return state;
});

vi.mock("../../src/reliability/timeouts.js", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../src/reliability/timeouts.js")>();

  return {
    ...actual,
    armTotalDurationTimeout: (
      ...args: Parameters<typeof actual.armTotalDurationTimeout>
    ) => {
      const armed = actual.armTotalDurationTimeout(...args);
      const controller = new AbortController();
      held.controller = controller;

      return {
        ...armed,
        signal: AbortSignal.any([armed.signal, controller.signal]),
      };
    },
  };
});

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};
const ONE_CONTENT_FRAME =
  'data: {"id": "c1", "choices": [{"delta": {"content": "Hel"}}]}\n\n';
const UNRECOGNIZED_REJECTION_SENTINEL =
  'data: {"error": {"message": "stream ended by an internal error", "type": "internal_error", "code": "unhandled_exception"}, "gateway": {"terminal": "UNRECOGNIZED_REJECTION"}}\n\n';
// Pino's base keys, the request id the route binds, and the log-field
// allowlist that a line about a rejection carries: the cause fields are
// always present, as UNKNOWN when the rejection has no cause.
const ALLOWED_LINE_KEYS = new Set([
  "level",
  "time",
  "pid",
  "hostname",
  "reqId",
  "req_id",
  "msg",
  "cause_code",
  "cause_name",
  "err_name",
  "abort_identity",
]);

function recordingBreaker(): {
  breaker: CircuitBreaker;
  recorded: ProbeOutcome[];
} {
  const recorded: ProbeOutcome[] = [];
  return {
    recorded,
    breaker: {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (outcome) => {
        recorded.push(outcome);
      },
      getState: () => "CLOSED",
    },
  };
}

// An accepted stream whose frames the test releases by hand; an abort of the
// request signal errors it with the signal's reason, as an undici body does.
function drivenAcceptedStream(signal: AbortSignal): {
  stream: AcceptedStream;
  release: (text: string) => void;
} {
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;

  const body = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
      signal.addEventListener(
        "abort",
        () => {
          c.error(signal.reason);
        },
        { once: true },
      );
    },
    pull() {
      return new Promise<void>(() => {});
    },
  });

  return {
    stream: { kind: "accepted_stream", status: 200, reader: body.getReader() },
    release: (text) => {
      if (controller === undefined) {
        throw new Error("the fake stream has not started");
      }
      controller.enqueue(new TextEncoder().encode(text));
    },
  };
}

async function buildDrivenApp() {
  const capture = makeLogCapture();
  const violations: string[] = [];
  const { breaker, recorded } = recordingBreaker();
  let driven: ReturnType<typeof drivenAcceptedStream> | undefined;

  const app = await buildProxyApp({
    breaker,
    logger: capture.logger,
    streamingEnabled: true,
    upstreamBuffered: bufferedTripwire(violations).seam,
    upstreamStreaming: (_body, signal) => {
      driven = drivenAcceptedStream(signal);
      return Promise.resolve(driven.stream);
    },
  });

  return {
    app,
    capture,
    violations,
    recorded,
    driven: () => {
      if (driven === undefined) {
        throw new Error("the streaming upstream was not called");
      }
      return driven;
    },
  };
}

describe("streaming flag ON: after the first-frame line, an abort that step 3 cannot interpret ends as UNRECOGNIZED_REJECTION", () => {
  it.each([
    {
      row: "the signal's own reason with a kind outside the funnel",
      reason: { kind: "not_a_funnel_kind" },
      line: { msg: "Unrecognized upstream rejection", abort_identity: true },
      rule: "a rejection the classifier does not recognize is never an upstream death",
    },
    {
      row: "the signal's own reason with a recognized kind that neither earlier check decides",
      reason: { kind: "wall_clock_expired" },
      line: { msg: "STREAM_REJECTION_OUTCOME_UNHANDLED", abort_identity: true },
      rule: "the defensive arm records and names, it never throws: an outcome step 3 has no row for is UNRECOGNIZED_REJECTION, never an upstream death",
    },
  ])("ends as UNRECOGNIZED_REJECTION when the read rejects with $row", async ({ reason, line, rule }) => {
    const t = await buildDrivenApp();
    vi.useFakeTimers();

    try {
      const req = t.app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });
      let ended = false;
      void req.then(() => {
        ended = true;
      });
      await vi.advanceTimersByTimeAsync(1_000);
      t.driven().release(ONE_CONTENT_FRAME);
      await vi.advanceTimersByTimeAsync(0);

      expect(t.violations).toStrictEqual([]);
      expect(
        t.capture.byEvent("stream_first_token"),
        "the scene's premise: the first frame crossed the first-frame line",
      ).toHaveLength(1);
      const controller = held.controller;
      if (controller === undefined) {
        throw new Error("the total-duration helper was not armed");
      }

      controller.abort(reason);
      await vi.advanceTimersByTimeAsync(0);

      expect(
        ended,
        `${rule}: the loop never leaves without a terminal`,
      ).toBe(true);
      const res = await req;
      expect(
        res.statusCode,
        "the status is sealed at 200 once the first frame is written",
      ).toBe(200);
      expect(
        t.capture.byEvent("stream_done"),
        "exactly one terminal event",
      ).toHaveLength(1);
      expect(t.capture.byEvent("stream_done")[0], rule).toMatchObject({
        completed: false,
        terminal: "UNRECOGNIZED_REJECTION",
        error_class: "gateway-fault",
      });
      expect(
        res.rawPayload,
        `${rule}: the stream ends with the forwarded frame and exactly one UNRECOGNIZED_REJECTION sentinel`,
      ).toStrictEqual(
        Buffer.from(ONE_CONTENT_FRAME + UNRECOGNIZED_REJECTION_SENTINEL, "utf8"),
      );
      expect(
        t.recorded,
        "the terminal votes INCONCLUSIVE, exactly once",
      ).toStrictEqual(["INCONCLUSIVE"]);

      const warnings = t.capture.logs.filter(
        (log) => typeof log.level === "number" && log.level >= 40,
      );
      expect(
        warnings.map((log) => ({ msg: log.msg, abort_identity: log.abort_identity })),
        `${rule}: one diagnostic line, and nothing is thrown to the framework's error handler`,
      ).toStrictEqual([line]);
      expect(
        new Set(Object.keys(warnings[0] ?? {})),
        "the diagnostic line carries only allowlisted fields: never an error message, a body or a credential",
      ).toEqual(ALLOWED_LINE_KEYS);
      expect(
        {
          cause_code: warnings[0]?.cause_code,
          cause_name: warnings[0]?.cause_name,
          err_name: warnings[0]?.err_name,
        },
        "the reason is a plain object with no cause, so every extracted name is the extractors' UNKNOWN",
      ).toStrictEqual({
        cause_code: "UNKNOWN",
        cause_name: "UNKNOWN",
        err_name: "UNKNOWN",
      });
    } finally {
      vi.useRealTimers();
      await t.app.close();
    }
  });
});
