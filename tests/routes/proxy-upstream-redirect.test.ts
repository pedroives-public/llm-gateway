import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { makeLogCapture, type LogCapture } from "../log-capture.js";

// A blocked upstream redirect is an operator-config fault: the consumer gets
// the deterministic sanitized 502, the breaker takes a FAILURE (the next call
// fails with certainty until the endpoint config changes), and no retry is
// spent. Unlike the unrecognized-rejection lane, the request stays visible to
// the SLIs through req_complete.

const SANITIZED_REDIRECT_BODY = {
  error: {
    message: "upstream redirect blocked",
    type: "server_error",
    code: "upstream_redirect_blocked",
  },
};

type ProbeResult = {
  res: Awaited<ReturnType<FastifyInstance["inject"]>>;
  recorded: ProbeOutcome[];
  upstreamCalls: number;
  capture: LogCapture;
};

async function injectScripted(script: Outcome[]): Promise<ProbeResult> {
  let upstreamCalls = 0;
  const scriptedUpstream = (): Promise<Outcome> => {
    const next = script[upstreamCalls] ?? script[script.length - 1];
    upstreamCalls += 1;
    if (next === undefined) {
      throw new Error("upstream script is empty");
    }
    return Promise.resolve(next);
  };
  const recorded: ProbeOutcome[] = [];
  const recordingBreaker: CircuitBreaker = {
    tryAcquire: () => ({ kind: "NORMAL" }),
    recordResult: (outcome) => {
      recorded.push(outcome);
    },
    getState: () => "CLOSED",
  };
  const capture = makeLogCapture();

  const app = await buildApp({
    logger: capture.logger,
    db: fakeAuthDb(randomUUID()),
    registerProtected: async (scope) => {
      await scope.register(proxyRoute, {
        breaker: recordingBreaker,
        upstreamBuffered: scriptedUpstream,
      });
    },
  });

  try {
    const res = await app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer() },
      payload: {
        model: "gpt-4o",
        messages: [{ role: "user", content: "hi" }],
      },
    });
    return { res, recorded, upstreamCalls, capture };
  } finally {
    await app.close();
  }
}

describe("upstream redirect blocked — sanitized terminal, breaker failure", () => {
  it("answers 502 upstream-redirect-blocked: sanitized body, single attempt, breaker FAILURE, req_complete emitted", async () => {
    const { res, recorded, upstreamCalls, capture } = await injectScripted([
      { kind: "redirect_blocked" },
    ]);

    expect(res.statusCode).toBe(502);
    expect(res.headers["x-gateway-error-class"]).toBe(
      "upstream-redirect-blocked",
    );
    expect(JSON.parse(res.payload)).toEqual(SANITIZED_REDIRECT_BODY);

    expect(upstreamCalls).toBe(1); // structurally retry-ineligible
    expect(recorded).toEqual(["FAILURE"]); // certain failure counts toward the breaker

    // The non-ok arm emits the SLI terminal: a blocked redirect is never
    // invisible the way the unrecognized 500 lane is.
    const complete = capture.byEvent("req_complete");
    expect(complete).toHaveLength(1);
    expect(complete[0]).toMatchObject({
      status: 502,
      error_class: "upstream-redirect-blocked",
      attempts: 1,
      retry_disposition: "ineligible",
    });
  });
});
