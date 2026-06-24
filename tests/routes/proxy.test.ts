import { randomBytes, randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it } from "vitest";
import { buildApp } from "../../src/app.js";
import { proxyRoute, type ChatCompletionsBody } from "../../src/routes/proxy.js";
import type {
  CircuitBreaker,
  ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import type { DrizzleClient } from "../../src/db/client.js";

const stubBreaker: CircuitBreaker = {
  tryAcquire: () => ({ kind: "NORMAL" }),
  recordResult: () => {},
  getState: () => "CLOSED",
};
const stubUpstreamBuffered = (): Promise<Outcome> =>
  Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });

// Fake db: one valid active row so an authenticated request reaches the route.
// Auth edge cases (bad hash, unknown key) live in tests/middleware/auth.test.ts.
function fakeAuthDb(tenantId: string): DrizzleClient {
  return {
    select() {
      return {
        from() {
          return {
            innerJoin() {
              return {
                where() {
                  return {
                    limit() {
                      return Promise.resolve([
                        {
                          tenantId,
                          planTier: "pro",
                          apiKeyStatus: "active",
                          tenantStatus: "active",
                        },
                      ]);
                    },
                  };
                },
              };
            },
          };
        },
      };
    },
  } as unknown as DrizzleClient;
}

describe("proxy route — buffered skeleton", () => {
  it("rejects a body missing `model` with 400 before any upstream call", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: stubUpstreamBuffered,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { messages: [{ role: "user", content: "hi" }] },
      });
      expect(res.statusCode).toBe(400);
    } finally {
      await app.close();
    }
  });

  it("forwards a buffered upstream 200 body verbatim (happy path)", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const upstreamBody = {
      id: "chatcmpl-1",
      choices: [{ message: { role: "assistant", content: "hello" } }],
    };
    const okUpstream = (): Promise<Outcome> =>
      Promise.resolve({ kind: "ok", status: 200, body_parsed: upstreamBody });

    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: okUpstream,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(upstreamBody);
    } finally {
      await app.close();
    }
  });

  it("fast-fails with 503 + circuit_breaker_open body, never calling upstream", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    // recordResult must never run on a fast-fail (no admitted attempt); throwing
    // here turns a stray call into a visible failure.
    const fastFailBreaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "FAST_FAIL" }),
      recordResult: () => {
        throw new Error("recordResult must not be called on FAST_FAIL");
      },
      getState: () => "OPEN",
    };

    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: fastFailBreaker,
          upstreamBuffered: countingUpstream,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      expect(res.statusCode).toBe(503);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(JSON.parse(res.payload)).toEqual({
        error: {
          message: "Service temporarily unavailable",
          type: "service_unavailable",
          code: "circuit_breaker_open",
        },
      });
      expect(upstreamCalls).toBe(0);
    } finally {
      await app.close();
    }
  });

  it("retry-exhausted 5xx → 502 upstream-retry-exhausted, body verbatim, breaker FAILURE", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const upstreamErrorBody =
      '{"error":{"message":"upstream boom","type":"server_error"}}';
    let upstreamCalls = 0;
    const failingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({
        kind: "upstream_error",
        status: 503,
        body_raw: upstreamErrorBody,
      });
    };
    const recorded: ProbeOutcome[] = [];
    const recordingBreaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (o) => {
        recorded.push(o);
      },
      getState: () => "CLOSED",
    };

    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: recordingBreaker,
          upstreamBuffered: failingUpstream,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      expect(res.statusCode).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(res.payload).toBe(upstreamErrorBody);
      expect(recorded).toEqual(["FAILURE"]);
      expect(upstreamCalls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it("passes a 4xx through verbatim: client-fault, breaker INCONCLUSIVE, no retry", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const body400 =
      '{"error":{"message":"bad model","type":"invalid_request_error"}}';
    let upstreamCalls = 0;
    const upstream400 = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({
        kind: "upstream_error",
        status: 400,
        body_raw: body400,
      });
    };
    const recorded: ProbeOutcome[] = [];
    const breaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (o) => {
        recorded.push(o);
      },
      getState: () => "CLOSED",
    };

    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker,
          upstreamBuffered: upstream400,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      // Inconclusive for the breaker: a client error is not upstream-health evidence.
      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(res.payload).toBe(body400);
      expect(recorded).toEqual(["INCONCLUSIVE"]);
      expect(upstreamCalls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it("passes a 429 through verbatim: upstream-retry-exhausted, breaker INCONCLUSIVE", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const body429 =
      '{"error":{"message":"rate limited","type":"rate_limit_error"}}';
    let upstreamCalls = 0;
    const upstream429 = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({
        kind: "upstream_error",
        status: 429,
        body_raw: body429,
      });
    };
    const recorded: ProbeOutcome[] = [];
    const breaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (o) => {
        recorded.push(o);
      },
      getState: () => "CLOSED",
    };

    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker,
          upstreamBuffered: upstream429,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      // Inconclusive for the breaker: Retry-After backpressure is not an availability failure.
      expect(res.statusCode).toBe(429);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(res.payload).toBe(body429);
      expect(recorded).toEqual(["INCONCLUSIVE"]);
      expect(upstreamCalls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it("undecodable upstream → 502 upstream-fault, normalized body, breaker FAILURE, no retry", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    let upstreamCalls = 0;
    const undecodableUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "undecodable" });
    };
    const recorded: ProbeOutcome[] = [];
    const breaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (o) => {
        recorded.push(o);
      },
      getState: () => "CLOSED",
    };

    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker,
          upstreamBuffered: undecodableUpstream,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      // No upstream body to forward -> synthesize a normalized body. A 2xx that
      // failed to decode proves execution, so it is not retry-eligible.
      expect(res.statusCode).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe("upstream-fault");
      expect(JSON.parse(res.payload)).toEqual({
        error: {
          message: "invalid response from upstream",
          type: "server_error",
          code: "upstream_decode_error",
        },
      });
      expect(recorded).toEqual(["FAILURE"]);
      expect(upstreamCalls).toBe(1);
    } finally {
      await app.close();
    }
  });

  // attempts are intentionally NOT asserted: these pin classify + body shaping,
  // identical whether or not retry fired.
  async function runBuffered(
    upstream: (b: ChatCompletionsBody, s: AbortSignal) => Promise<Outcome>,
  ) {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const recorded: ProbeOutcome[] = [];
    const breaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (o) => {
        recorded.push(o);
      },
      getState: () => "CLOSED",
    };
    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker,
          upstreamBuffered: upstream,
        });
      },
    });
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      return { res, recorded };
    } finally {
      await app.close();
    }
  }

  it("network_failed pre-send → 502 upstream-retry-exhausted + FAILURE (proposed body)", async () => {
    const { res, recorded } = await runBuffered(() =>
      Promise.resolve({
        kind: "network_failed",
        pre_send_proven: true,
        cause_code: "ECONNREFUSED",
        cause_name: "Error",
      }),
    );
    expect(res.statusCode).toBe(502);
    expect(res.headers["x-gateway-error-class"]).toBe("upstream-retry-exhausted");
    expect(JSON.parse(res.payload)).toEqual({
      error: {
        message: "upstream unavailable",
        type: "server_error",
        code: "upstream_unavailable",
      },
    });
    expect(recorded).toEqual(["FAILURE"]);
  });

  it("network_failed post-send → 504 gateway-fault + FAILURE (proposed body)", async () => {
    const { res, recorded } = await runBuffered(() =>
      Promise.resolve({
        kind: "network_failed",
        pre_send_proven: false,
        cause_code: "ECONNRESET",
        cause_name: "Error",
      }),
    );
    expect(res.statusCode).toBe(504);
    expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
    expect(JSON.parse(res.payload)).toEqual({
      error: {
        message: "gateway error",
        type: "gateway_error",
        code: "upstream_connection_failed",
      },
    });
    expect(recorded).toEqual(["FAILURE"]);
  });

  it("aborted wall_clock_expired → 504 gateway-fault + FAILURE (spec-pinned body)", async () => {
    const { res, recorded } = await runBuffered(() =>
      Promise.resolve({ kind: "aborted", abort_kind: "wall_clock_expired" }),
    );
    expect(res.statusCode).toBe(504);
    expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
    expect(JSON.parse(res.payload)).toEqual({
      error: {
        message: "gateway timeout",
        type: "gateway_timeout",
        code: "wall_clock_exceeded",
      },
    });
    expect(recorded).toEqual(["FAILURE"]);
  });

  it("aborted response_size_cap → 502 upstream-fault + INCONCLUSIVE (proposed body)", async () => {
    const { res, recorded } = await runBuffered(() =>
      Promise.resolve({ kind: "aborted", abort_kind: "response_size_cap" }),
    );
    expect(res.statusCode).toBe(502);
    expect(res.headers["x-gateway-error-class"]).toBe("upstream-fault");
    expect(JSON.parse(res.payload)).toEqual({
      error: {
        message: "upstream response too large",
        type: "server_error",
        code: "response_too_large",
      },
    });
    expect(recorded).toEqual(["INCONCLUSIVE"]);
  });
});

describe("proxy route — default wiring (e2e)", () => {
  it("serves a request end-to-end through buildApp's real breaker + client", async () => {
    const upstreamBody = { id: "chatcmpl-e2e", choices: [{ index: 0 }] };
    const server = http.createServer((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(upstreamBody));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;

    const prevBaseUrl = process.env["OPENAI_BASE_URL"];
    process.env["OPENAI_BASE_URL"] = `http://127.0.0.1:${port}`;

    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    // No registerProtected → buildApp wires the real breaker + real client.
    const app = await buildApp({ logger: false, db: fakeAuthDb(tenantId) });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
      });
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(upstreamBody);
    } finally {
      await app.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      if (prevBaseUrl === undefined) {
        delete process.env["OPENAI_BASE_URL"];
      } else {
        process.env["OPENAI_BASE_URL"] = prevBaseUrl;
      }
    }
  });
});

describe("proxy route — error desk (scoped setErrorHandler)", () => {
  // Auth (onRequest) runs before body parsing and schema validation, so a valid
  // key is needed to reach the 413/400 paths.
  async function buildWithUpstream(
    upstreamBuffered: (b: ChatCompletionsBody, s: AbortSignal) => Promise<Outcome>,
  ) {
    const tenantId = randomUUID();
    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered,
        });
      },
    });
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    return { app, apiKey };
  }

  it("413 body-too-large → OpenAI shape + client-fault, no default Fastify body", async () => {
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    const { app, apiKey } = await buildWithUpstream(countingUpstream);
    try {
      // > 256 KiB once serialized; the route bodyLimit rejects it pre-handler.
      const big = "x".repeat(300_000);
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: big }] },
      });
      expect(res.statusCode).toBe(413);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(JSON.parse(res.payload)).toEqual({
        error: {
          message: expect.any(String),
          type: "invalid_request_error",
          code: "request_too_large",
        },
      });
      // Guard: the default Fastify body (`error: "Payload Too Large"`) must not leak.
      expect(res.payload).not.toContain("Payload Too Large");
      expect(upstreamCalls).toBe(0);
    } finally {
      await app.close();
    }
  });

  it("400 missing `model` → invalid_request_error + code model_missing, no upstream call", async () => {
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    const { app, apiKey } = await buildWithUpstream(countingUpstream);
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { messages: [{ role: "user", content: "hi" }] },
      });
      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      const body = JSON.parse(res.payload) as {
        error: { type: string; code: string };
      };
      expect(body.error.type).toBe("invalid_request_error");
      expect(body.error.code).toBe("model_missing");
      expect(upstreamCalls).toBe(0);
    } finally {
      await app.close();
    }
  });

  it("400 empty `messages` → code messages_empty", async () => {
    const { app, apiKey } = await buildWithUpstream(stubUpstreamBuffered);
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [] },
      });
      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      const body = JSON.parse(res.payload) as {
        error: { type: string; code: string };
      };
      expect(body.error.type).toBe("invalid_request_error");
      expect(body.error.code).toBe("messages_empty");
    } finally {
      await app.close();
    }
  });

  it("unhandled exception → 500 gateway-fault + unhandled_exception, no leak in body", async () => {
    const throwingUpstream = (): Promise<Outcome> => {
      throw new Error("boom: simulated handler bug");
    };
    const { app, apiKey } = await buildWithUpstream(throwingUpstream);
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      });
      expect(res.statusCode).toBe(500);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(JSON.parse(res.payload)).toEqual({
        error: {
          message: "internal server error",
          type: "internal_error",
          code: "unhandled_exception",
        },
      });
      // The thrown error's message/stack must not reach the client body.
      expect(res.payload).not.toContain("boom");
    } finally {
      await app.close();
    }
  });
});
