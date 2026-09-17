import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { CircuitBreaker } from "../../src/reliability/circuit-breaker.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { buildProxyApp } from "../helpers/proxy-app.js";

// The streaming flag decides the request schema once, at route registration.
// OFF keeps the buffered-only contract: `stream: true` is rejected at the
// schema. ON accepts a boolean `stream`.
//
// No streaming branch exists yet, so the ON cells stop the request at the
// circuit breaker: a breaker that refuses admission answers 503 before any
// upstream call. That proves the body passed the schema without sending a
// `stream: true` body down the buffered path.

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};

// Refuses every admission. A refused request has no upstream result, so any
// attempt to record one is a visible failure.
const refusingBreaker: CircuitBreaker = {
  tryAcquire: () => ({ kind: "FAST_FAIL" }),
  recordResult: () => {
    throw new Error(
      "recordResult must not run: a request refused by the breaker has no upstream result",
    );
  },
  getState: () => "OPEN",
};

function countingUpstream(): {
  calls: () => number;
  buffered: () => Promise<Outcome>;
} {
  let calls = 0;
  return {
    calls: () => calls,
    buffered: () => {
      calls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    },
  };
}

async function post(
  options: { streamingEnabled: boolean; breaker: CircuitBreaker },
  payload: Record<string, unknown>,
): Promise<{ status: number; code: unknown; upstreamCalls: number }> {
  const upstream = countingUpstream();
  const app = await buildProxyApp({
    breaker: options.breaker,
    streamingEnabled: options.streamingEnabled,
    upstreamBuffered: upstream.buffered,
  });

  try {
    const res = await app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer() },
      payload,
    });
    const body = res.json<{ error?: { code?: unknown } }>();
    return {
      status: res.statusCode,
      code: body.error?.code,
      upstreamCalls: upstream.calls(),
    };
  } finally {
    await app.close();
  }
}

describe("streaming flag: the schema is built from the flag", () => {
  // Every row runs against the refusing breaker, so between the two
  // `stream: true` rows the flag is the only thing that changes.
  it.each([
    {
      streamingEnabled: false,
      stream: true,
      expected: { status: 400, code: "stream_not_supported", upstreamCalls: 0 },
    },
    {
      streamingEnabled: true,
      stream: true,
      expected: { status: 503, code: "circuit_breaker_open", upstreamCalls: 0 },
    },
    {
      streamingEnabled: false,
      stream: "x",
      expected: { status: 400, code: "invalid_request", upstreamCalls: 0 },
    },
    {
      streamingEnabled: true,
      stream: "x",
      expected: { status: 400, code: "invalid_request", upstreamCalls: 0 },
    },
  ])(
    "flag $streamingEnabled, stream $stream -> $expected.status $expected.code",
    async ({ streamingEnabled, stream, expected }) => {
      const outcome = await post(
        { streamingEnabled, breaker: refusingBreaker },
        { ...validBody, stream },
      );

      expect(
        outcome,
        "streaming flag: OFF rejects stream:true at the schema, ON lets it reach the breaker, and a non-boolean stream is an ordinary invalid request in both states",
      ).toStrictEqual(expected);
    },
  );

  it.each([{ stream: false }, {}])(
    "flag ON still serves a buffered request (payload %j)",
    async (extra) => {
      const outcome = await post(
        { streamingEnabled: true, breaker: stubBreaker },
        { ...validBody, ...extra },
      );

      expect(
        outcome,
        "streaming flag: ON must not block a request whose stream is false or absent",
      ).toStrictEqual({ status: 200, code: undefined, upstreamCalls: 1 });
    },
  );
});

describe("streaming flag: the route ignores the process environment", () => {
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env["STREAMING_ENABLED"];
  });

  afterEach(() => {
    if (saved !== undefined) {
      process.env["STREAMING_ENABLED"] = saved;
    } else {
      delete process.env["STREAMING_ENABLED"];
    }
  });

  // The environment is set to the OPPOSITE of the option on purpose. This is
  // not a way to switch states: it proves the environment cannot switch them.
  it.each([
    {
      env: "true",
      streamingEnabled: false,
      expected: { status: 400, code: "stream_not_supported", upstreamCalls: 0 },
    },
    {
      env: "false",
      streamingEnabled: true,
      expected: { status: 503, code: "circuit_breaker_open", upstreamCalls: 0 },
    },
  ])(
    "env $env with option $streamingEnabled -> the option wins",
    async ({ env, streamingEnabled, expected }) => {
      process.env["STREAMING_ENABLED"] = env;

      const outcome = await post(
        { streamingEnabled, breaker: refusingBreaker },
        { ...validBody, stream: true },
      );

      expect(
        outcome,
        "streaming flag: the route takes the flag from its options and must never read process.env",
      ).toStrictEqual(expected);
    },
  );
});
