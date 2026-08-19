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

// Quota exhaustion is the deployment operator's account condition, never the
// consumer's: the upstream 429 body that reveals it (error.code
// insufficient_quota) is replaced by a deterministic sanitized 502 and never
// retried. The legitimate rate-limit sibling keeps its verbatim passthrough
// (the discrimination must not widen); under default-deny neither
// sibling retries — the discrimination now lives in status/body/class only.

const SANITIZED_QUOTA_BODY = {
  error: {
    message: "upstream quota exhausted",
    type: "server_error",
    code: "upstream_quota_exhausted",
  },
};

const QUOTA_BODY_RAW = JSON.stringify({
  error: {
    message:
      "You exceeded your current quota, please check your plan and billing details.",
    type: "insufficient_quota",
    param: null,
    code: "insufficient_quota",
  },
});

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

describe("upstream 429 insufficient_quota — sanitized, never passed through", () => {
  it("sanitizes to 502 upstream-quota-exhausted: no body/Retry-After leak, single attempt, breaker-neutral", async () => {
    const { res, recorded, upstreamCalls, capture } = await injectScripted([
      {
        kind: "upstream_error",
        status: 429,
        body_raw: QUOTA_BODY_RAW,
        retry_after: "7",
      },
    ]);

    expect(res.statusCode).toBe(502);
    expect(res.headers["x-gateway-error-class"]).toBe(
      "upstream-quota-exhausted",
    );
    // The Retry-After would advertise the operator's billing recovery; it and
    // the upstream body never reach the consumer, in ANY disposition.
    expect(res.headers["retry-after"]).toBeUndefined();
    expect(JSON.parse(res.payload)).toEqual(SANITIZED_QUOTA_BODY);
    expect(res.payload).not.toContain("billing");
    expect(res.payload).not.toContain("insufficient_quota");

    expect(upstreamCalls).toBe(1); // retry-ineligible: exactly one attempt
    expect(recorded).toEqual(["INCONCLUSIVE"]); // body-derived: never a breaker FAILURE

    const complete = capture.byEvent("req_complete");
    expect(complete).toHaveLength(1);
    expect(complete[0]).toMatchObject({
      status: 502,
      error_class: "upstream-quota-exhausted",
      attempts: 1,
      retry_disposition: "ineligible",
    });
  });

  it(
    "keeps the legitimate rate-limit 429 on verbatim passthrough without retry — discrimination does not widen",
    { timeout: 15_000 },
    async () => {
      const rateLimitBody = JSON.stringify({
        error: {
          message: "Rate limit reached for gpt-4o",
          type: "requests",
          code: "rate_limit_exceeded",
        },
      });
      // A second scripted 429 sits behind the first: an eligibility
      // regression would fetch it and fail the single-attempt pin below.
      const { res, upstreamCalls, capture } = await injectScripted([
        { kind: "upstream_error", status: 429, body_raw: rateLimitBody },
        { kind: "upstream_error", status: 429, body_raw: rateLimitBody },
      ]);

      expect(res.statusCode).toBe(429);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(res.payload).toBe(rateLimitBody); // verbatim, byte-identical
      expect(upstreamCalls).toBe(1); // default-deny: one attempt only

      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        status: 429,
        error_class: "upstream-retry-exhausted",
        attempts: 1,
        retry_disposition: "ineligible",
      });
    },
  );
});
