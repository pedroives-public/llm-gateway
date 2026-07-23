import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import {
  createCircuitBreaker,
  type CircuitBreaker,
  type ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { makeLogCapture, type LogCapture } from "../log-capture.js";

// Upstream credential rejections: the upstream credential belongs to the
// deployment operator, never the consumer, so the upstream's status, headers
// and body are replaced by a deterministic sanitized 502. A 401 counts toward
// the breaker (delta 1): with auth headers consumer-isolated (adapter
// header-isolation pin), a 401 can only mean the deployment's own credential
// was rejected — a persistent operator-side condition.

const SANITIZED_AUTH_BODY = {
  error: {
    message: "upstream authentication failed",
    type: "server_error",
    code: "upstream_auth_failure",
  },
};

type ProbeResult = {
  res: Awaited<ReturnType<FastifyInstance["inject"]>>;
  recorded: ProbeOutcome[];
  upstreamCalls: number;
  capture: LogCapture;
};

async function inject401(
  bodyRaw: string,
  retryAfter?: string,
): Promise<ProbeResult> {
  let upstreamCalls = 0;
  const upstream401 = (): Promise<Outcome> => {
    upstreamCalls += 1;
    return Promise.resolve({
      kind: "upstream_error",
      status: 401,
      body_raw: bodyRaw,
      ...(retryAfter === undefined ? {} : { retry_after: retryAfter }),
    });
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
        upstreamBuffered: upstream401,
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

describe("upstream 401 — sanitized credential rejection", () => {
  it("sanitizes to 502 upstream-auth-failure: no upstream body/header leak, breaker FAILURE, no retry", async () => {
    const secretBearing =
      '{"error":{"message":"Incorrect API key provided: sk-live-SECRETSECRET"}}';
    const { res, recorded, upstreamCalls, capture } = await inject401(
      secretBearing,
      "7",
    );

    expect(res.statusCode).toBe(502);
    expect(res.headers["x-gateway-error-class"]).toBe("upstream-auth-failure");
    expect(res.headers["retry-after"]).toBeUndefined();
    expect(JSON.parse(res.payload)).toEqual(SANITIZED_AUTH_BODY);
    expect(res.payload).not.toContain("sk-");
    expect(res.payload).not.toContain("SECRET");

    expect(recorded).toEqual(["FAILURE"]);
    expect(upstreamCalls).toBe(1); // 401 is retry-ineligible: exactly one attempt

    const complete = capture.byEvent("req_complete");
    expect(complete).toHaveLength(1);
    expect(complete[0]).toMatchObject({
      status: 502,
      error_class: "upstream-auth-failure",
      attempts: 1,
      retry_disposition: "ineligible",
    });
  });

  it("returns the byte-identical sanitized body for JSON, plaintext and malformed upstream payloads", async () => {
    const payloads = [
      '{"error":{"message":"json shape"}}',
      "plain text unauthorized",
      "{not json at all",
    ];
    const bodies: string[] = [];
    for (const payload of payloads) {
      const { res } = await inject401(payload);
      expect(res.statusCode).toBe(502);
      bodies.push(res.payload);
    }
    expect(new Set(bodies).size).toBe(1);
    const [first] = bodies;
    if (first === undefined) {
      throw new Error("no sanitized bodies were collected");
    }
    expect(JSON.parse(first)).toEqual(SANITIZED_AUTH_BODY);
  });
});

const SANITIZED_ACCESS_BODY = {
  error: {
    message: "upstream access denied",
    type: "server_error",
    code: "upstream_access_denied",
  },
};

describe("upstream 403 — sanitized access denial (breaker-neutral)", () => {
  it("a 403 storm sanitizes every response to 502 and never opens the REAL breaker", async () => {
    const cbEvents: object[] = [];
    const breaker = createCircuitBreaker({ info: (o) => cbEvents.push(o) });
    let upstreamCalls = 0;
    const upstream403 = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({
        kind: "upstream_error",
        status: 403,
        body_raw:
          '{"error":{"message":"Project org-INTERNAL does not have access to model"}}',
      });
    };
    const capture = makeLogCapture();

    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(randomUUID()),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker,
          upstreamBuffered: upstream403,
        });
      },
    });

    try {
      // A consumer can induce 403s at will via the free-form model field; the
      // breaker is deployment-shared, so six delta-0 terminals inside one
      // 30 s window must leave the FSM untouched.
      for (let i = 1; i <= 6; i++) {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: {
            model: "model-outside-account-scope",
            messages: [{ role: "user", content: "hi" }],
          },
        });
        expect(res.statusCode, `storm request ${i}`).toBe(502);
        expect(res.headers["x-gateway-error-class"]).toBe(
          "upstream-access-denied",
        );
        expect(res.headers["retry-after"]).toBeUndefined();
        expect(JSON.parse(res.payload)).toEqual(SANITIZED_ACCESS_BODY);
        expect(res.payload).not.toContain("org-INTERNAL");
      }

      expect(breaker.getState()).toBe("CLOSED");
      expect(cbEvents).toEqual([]); // no state transition ever fired

      // The breaker never opened, so the next request still reaches upstream.
      const after = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: {
          model: "model-outside-account-scope",
          messages: [{ role: "user", content: "hi" }],
        },
      });
      expect(after.statusCode).toBe(502);
      expect(upstreamCalls).toBe(7); // one attempt per request: 403 never retries

      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(7);
      expect(complete[0]).toMatchObject({
        status: 502,
        error_class: "upstream-access-denied",
        attempts: 1,
        retry_disposition: "ineligible",
      });
    } finally {
      await app.close();
    }
  });
});

describe("retry-then-401 — credential classification survives the retry path", () => {
  it(
    "500 then 401: terminal 502 upstream-auth-failure, attempts 2, breaker FAILURE",
    { timeout: 15_000 },
    async () => {
      // Guards a composition regression: "retries exhausted" must not become
      // the terminal class when the second attempt is a credential rejection —
      // that would route the 401 through the verbatim-passthrough branch.
      const scripted: Outcome[] = [
        {
          kind: "upstream_error",
          status: 500,
          body_raw: '{"error":{"message":"boom"}}',
        },
        {
          kind: "upstream_error",
          status: 401,
          body_raw: '{"error":{"message":"bad key sk-live-SECRETSECRET"}}',
        },
      ];
      let upstreamCalls = 0;
      const sequencedUpstream = (): Promise<Outcome> => {
        const next = scripted[upstreamCalls];
        upstreamCalls += 1;
        if (next === undefined) {
          throw new Error("upstream called more times than scripted");
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
            upstreamBuffered: sequencedUpstream,
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

        expect(res.statusCode).toBe(502);
        expect(res.headers["x-gateway-error-class"]).toBe(
          "upstream-auth-failure",
        );
        expect(JSON.parse(res.payload)).toEqual(SANITIZED_AUTH_BODY);
        expect(res.payload).not.toContain("sk-");
        expect(upstreamCalls).toBe(2);
        expect(recorded).toEqual(["FAILURE"]);

        const complete = capture.byEvent("req_complete");
        expect(complete).toHaveLength(1);
        expect(complete[0]).toMatchObject({
          status: 502,
          error_class: "upstream-auth-failure",
          attempts: 2,
          retry_disposition: "attempted",
        });
      } finally {
        await app.close();
      }
    },
  );
});
