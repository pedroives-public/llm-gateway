import { randomBytes, randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it, vi } from "vitest";
import { buildApp } from "../../src/app.js";
import {
  proxyRoute,
  deriveRetryDisposition,
  type ChatCompletionsBody,
} from "../../src/routes/proxy.js";
import {
  createCircuitBreaker,
  type CircuitBreaker,
  type ProbeOutcome,
} from "../../src/reliability/circuit-breaker.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { resolveRejection, type Logger } from "../../src/upstream/rejection.js";
import { makeLogCapture, type LogCapture } from "../log-capture.js";
import { fetchFailed } from "../helpers/fetch-rejection.js";
import { fakeAuthDb } from "../helpers/fake-auth-db.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";

const stubUpstreamBuffered = (): Promise<Outcome> =>
  Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });

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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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
    const capture = makeLogCapture();

    const app = await buildApp({
      logger: capture.logger,
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
      });
      expect(res.statusCode).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(res.payload).toBe(upstreamErrorBody);
      expect(recorded).toEqual(["FAILURE"]);
      expect(upstreamCalls).toBe(2);

      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        attempts: 2,
        error_class: "upstream-retry-exhausted",
      });
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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
    expect(res.headers["x-gateway-error-class"]).toBe(
      "upstream-retry-exhausted",
    );
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
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
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
    upstreamBuffered: (
      b: ChatCompletionsBody,
      s: AbortSignal,
    ) => Promise<Outcome>,
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: big }],
        },
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

  it("400 null/empty `model` → code model_empty, no upstream call", async () => {
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    const { app, apiKey } = await buildWithUpstream(countingUpstream);
    try {
      for (const model of [null, ""]) {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: `Bearer ${apiKey}` },
          payload: { model, messages: [{ role: "user", content: "hi" }] },
        });
        expect(res.statusCode, `model -> ${JSON.stringify(model)}`).toBe(400);
        expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
        const body = JSON.parse(res.payload) as {
          error: { type: string; code: string };
        };
        expect(body.error.type).toBe("invalid_request_error");
        expect(body.error.code).toBe("model_empty");
      }
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
        payload: {
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
        },
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

describe("deriveRetryDisposition — cause discriminant (unit)", () => {
  const notAborted = new AbortController().signal;

  it("budget-skip WITHOUT a valid Retry-After is ineligible, not skipped_budget", () => {
    // Eligible 503, not aborted, but no Retry-After: the skip would be
    // backoff-driven, not backpressure → ineligible.
    const outcome: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "{}",
    };
    expect(deriveRetryDisposition(1, outcome, notAborted)).toBe("ineligible");
  });

  it("budget-skip with a PRESENT but unparseable Retry-After is ineligible", () => {
    // Present but unusable (an HTTP-date, not delta-seconds): the case that pins
    // validity over presence — a `retry_after !== undefined` check mislabels it
    // skipped_budget.
    const outcome: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "{}",
      retry_after: "Wed, 21 Oct 2099 07:28:00 GMT",
    };
    expect(deriveRetryDisposition(1, outcome, notAborted)).toBe("ineligible");
  });

  it("budget-skip with a non-positive Retry-After is ineligible", () => {
    // "0"/negative parse but never govern a wait (retry.ts requires > 0), so
    // they are not a Retry-After skip — pins the positivity half of "valid".
    const outcome: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "{}",
      retry_after: "0",
    };
    expect(deriveRetryDisposition(1, outcome, notAborted)).toBe("ineligible");
  });

  it("budget-skip WITH a valid Retry-After is skipped_budget", () => {
    const outcome: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "{}",
      retry_after: "31",
    };
    expect(deriveRetryDisposition(1, outcome, notAborted)).toBe(
      "skipped_budget",
    );
  });
});

describe("proxy route — reliability integration (8.1 harness)", () => {
  function recordingBreaker(recorded: ProbeOutcome[]): CircuitBreaker {
    return {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: (o) => {
        recorded.push(o);
      },
      getState: () => "CLOSED",
    };
  }

  async function buildWith(
    breaker: CircuitBreaker,
    upstreamBuffered: (
      b: ChatCompletionsBody,
      s: AbortSignal,
      log: Logger,
    ) => Promise<Outcome>,
    capture?: LogCapture,
  ) {
    const tenantId = randomUUID();
    const app = await buildApp({
      logger: capture?.logger ?? false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, { breaker, upstreamBuffered });
      },
    });
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    return { app, apiKey };
  }

  const validBody = {
    model: "gpt-4o",
    messages: [{ role: "user", content: "hi" }],
  };

  it("8.3 success: 200 verbatim + req_complete{attempts:1, error_class:null} (log-capture)", async () => {
    const capture = makeLogCapture();
    const okBody = { id: "chatcmpl-8-3", choices: [{ index: 0 }] };
    const recorded: ProbeOutcome[] = [];
    const upstream = (): Promise<Outcome> =>
      Promise.resolve({ kind: "ok", status: 200, body_parsed: okBody });

    const tenantId = randomUUID();
    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: recordingBreaker(recorded),
          upstreamBuffered: upstream,
        });
      },
    });
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(okBody);

      // Assert the terminal req_complete event, not just the body.
      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        attempts: 1,
        error_class: null,
        retry_disposition: "ineligible",
      });
    } finally {
      await app.close();
    }
  });

  it("8.5 retry-then-success: 503 then 200 → 200, two attempts, breaker not incremented", async () => {
    let calls = 0;
    const okBody = { id: "chatcmpl-retry", choices: [{ index: 0 }] };
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    // Sequence fake: attempt 1 fails retry-eligibly (503), attempt 2 succeeds.
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve(
        calls === 1
          ? {
              kind: "upstream_error",
              status: 503,
              body_raw: '{"error":{"message":"transient"}}',
            }
          : { kind: "ok", status: 200, body_parsed: okBody },
      );
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(okBody);
      expect(calls).toBe(2); // retry fired → two upstream attempts
      expect(recorded).toEqual(["SUCCESS"]); // success-on-retry: breaker not incremented

      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        attempts: 2,
        error_class: null,
        retry_disposition: "attempted",
      });
    } finally {
      await app.close();
    }
  });

  it(
    "8.7 CB OPEN fast-fail: five retry-exhausted requests open the REAL breaker; the sixth fast-fails with attempts: 0",
    { timeout: 15_000 },
    async () => {
      const capture = makeLogCapture();
      const cbEvents: object[] = [];
      const breaker = createCircuitBreaker({ info: (o) => cbEvents.push(o) });
      let upstreamCalls = 0;
      const failing503 = (): Promise<Outcome> => {
        upstreamCalls += 1;
        return Promise.resolve({
          kind: "upstream_error",
          status: 503,
          body_raw: '{"error":{"message":"down","type":"server_error"}}',
        });
      };
      const { app, apiKey } = await buildWith(breaker, failing503, capture);

      try {
        // Prime the real FSM: five retry-exhausted requests (two attempts
        // each), the fifth FAILURE crosses the open threshold.
        for (let i = 1; i <= 5; i++) {
          const res = await app.inject({
            method: "POST",
            url: "/v1/chat/completions",
            headers: { authorization: `Bearer ${apiKey}` },
            payload: validBody,
          });
          expect(res.statusCode, `priming request ${i}`).toBe(502);
        }
        expect(upstreamCalls).toBe(10); // two attempts per primed request
        expect(breaker.getState()).toBe("OPEN");

        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: `Bearer ${apiKey}` },
          payload: validBody,
        });
        expect(res.statusCode).toBe(503);
        expect(res.headers["x-gateway-error-class"]).toBe(
          "upstream-retry-exhausted",
        );
        expect(JSON.parse(res.payload)).toMatchObject({
          error: { code: "circuit_breaker_open" },
        });

        expect(upstreamCalls).toBe(10);

        expect(cbEvents).toHaveLength(1);
        expect(cbEvents[0]).toMatchObject({
          event: "cb_state_change",
          from: "CLOSED",
          to: "OPEN",
        });

        const complete = capture.byEvent("req_complete");
        expect(complete).toHaveLength(6); // one terminal per request
        expect(complete.at(-1)).toMatchObject({
          attempts: 0,
          error_class: "upstream-retry-exhausted",
          retry_disposition: "ineligible",
        });
      } finally {
        await app.close();
      }
    },
  );

  it("8.10 wall-clock: upstream hangs past 30s → 504 gateway-fault", async () => {
    const recorded: ProbeOutcome[] = [];
    // Hangs until the gateway's own AbortSignal fires, then resolves to the
    // aborted Outcome the real client would synthesize (mirrors openai.ts).
    const upstream = (
      _b: ChatCompletionsBody,
      signal: AbortSignal,
    ): Promise<Outcome> =>
      new Promise((resolve) => {
        signal.addEventListener(
          "abort",
          () => resolve({ kind: "aborted", abort_kind: "wall_clock_expired" }),
          { once: true },
        );
      });
    // Build with real timers (Fastify boot), then switch to fake timers for the
    // request so advanceTimersByTimeAsync deterministically fires the 30s deadline.
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
    );
    vi.useFakeTimers();
    try {
      const injected = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      await vi.advanceTimersByTimeAsync(30_000);
      const res = await injected;
      expect(res.statusCode).toBe(504);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(
        (JSON.parse(res.payload) as { error: { code: string } }).error.code,
      ).toBe("wall_clock_exceeded");
      expect(recorded).toEqual(["FAILURE"]); // wall-clock abort increments the breaker
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });

  it("8.15 at-most-once: post-send network failure → 504, single attempt (no retry)", async () => {
    let calls = 0;
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    // pre_send_proven false: bytes may have reached upstream, so the final state is unknown → not retry-eligible (at-most-once).
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve({
        kind: "network_failed",
        pre_send_proven: false,
        cause_code: "ECONNRESET",
        cause_name: "Error",
      });
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(504);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(calls).toBe(1); // a post-send failure is never retried
      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        attempts: 1,
        error_class: "gateway-fault",
        retry_disposition: "ineligible",
      });
    } finally {
      await app.close();
    }
  });

  it("8.14 Idempotency-Key is a no-op: post-send failure → 504, single attempt despite the header", async () => {
    let calls = 0;
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve({
        kind: "network_failed",
        pre_send_proven: false,
        cause_code: "ECONNRESET",
        cause_name: "Error",
      });
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: `Bearer ${apiKey}`,
          "idempotency-key": "11111111-1111-1111-1111-111111111111",
        },
        payload: validBody,
      });
      // Identical to the no-key case (504, one attempt): the header must not retry.
      expect(res.statusCode).toBe(504);
      expect(calls).toBe(1);
      // req_start proves the header was seen — yet it changed nothing.
      expect(capture.byEvent("req_start")[0]).toMatchObject({
        idempotency_key_present: true,
      });
      expect(capture.byEvent("req_complete")[0]).toMatchObject({
        attempts: 1,
        error_class: "gateway-fault",
        retry_disposition: "ineligible",
      });
    } finally {
      await app.close();
    }
  });

  it("8.17 pre-send failure retried: ECONNREFUSED then 200 → 200, two attempts", async () => {
    let calls = 0;
    const okBody = { id: "chatcmpl-8-17", choices: [{ index: 0 }] };
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    // pre_send_proven true: no bytes reached upstream, so a retry is side-effect-free.
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve(
        calls === 1
          ? {
              kind: "network_failed",
              pre_send_proven: true,
              cause_code: "ECONNREFUSED",
              cause_name: "Error",
            }
          : { kind: "ok", status: 200, body_parsed: okBody },
      );
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(okBody);
      expect(calls).toBe(2); // pre-send proof authorizes exactly one retry
      expect(recorded).toEqual(["SUCCESS"]);
      expect(capture.byEvent("req_complete")[0]).toMatchObject({
        attempts: 2,
        error_class: null,
        retry_disposition: "attempted",
      });
    } finally {
      await app.close();
    }
  });

  it("8.17 pre-send failure exhausted: ECONNREFUSED twice → 502 upstream-retry-exhausted", async () => {
    let calls = 0;
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve({
        kind: "network_failed",
        pre_send_proven: true,
        cause_code: "ECONNREFUSED",
        cause_name: "Error",
      });
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(502);
      expect(res.headers["x-gateway-error-class"]).toBe(
        "upstream-retry-exhausted",
      );
      expect(calls).toBe(2); // retried once; both attempts failed pre-send
      expect(recorded).toEqual(["FAILURE"]);
      expect(capture.byEvent("req_complete")[0]).toMatchObject({
        attempts: 2,
        error_class: "upstream-retry-exhausted",
        retry_disposition: "attempted",
      });
    } finally {
      await app.close();
    }
  });

  it("8.18 Retry-After honored: 429 + Retry-After:2 then 200 → waits the delay, then 200", async () => {
    let calls = 0;
    const okBody = { id: "chatcmpl-8-18", choices: [{ index: 0 }] };
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve(
        calls === 1
          ? {
              kind: "upstream_error",
              status: 429,
              body_raw: '{"error":{"message":"rate limited"}}',
              retry_after: "2",
            }
          : { kind: "ok", status: 200, body_parsed: okBody },
      );
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    // Build under real timers (Fastify boot), then drive the Retry-After wait
    // with fake timers so the 2s delay is deterministic, not wall-clock.
    vi.useFakeTimers();
    try {
      const injected = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      // The second attempt must not fire until the full Retry-After elapses.
      await vi.advanceTimersByTimeAsync(1999);
      expect(calls).toBe(1); // still waiting out the 2s
      await vi.advanceTimersByTimeAsync(1);
      const res = await injected;
      // 200 (not 504) proves the 2s wait fit the 30s budget — wall-clock never fired.
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(okBody);
      expect(calls).toBe(2);
      expect(recorded).toEqual(["SUCCESS"]); // 429-then-200 is a success, no breaker delta
      expect(capture.byEvent("req_complete")[0]).toMatchObject({
        attempts: 2,
        error_class: null,
        retry_disposition: "attempted",
      });
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });

  it("8.43 Retry-After budget-skip: 503 + Retry-After exceeding the wall-clock → passthrough 503, single attempt, breaker not incremented", async () => {
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    const body503 = '{"error":{"message":"overloaded","type":"server_error"}}';
    let calls = 0;
    // Retry-After "31" can't fit the 30s budget → retry skipped, attempt one stands.
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve({
        kind: "upstream_error",
        status: 503,
        body_raw: body503,
        retry_after: "31",
      });
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      // Passthrough verbatim — not a normalized 502, not a manufactured 504.
      expect(res.statusCode).toBe(503);
      expect(res.payload).toBe(body503);
      expect(res.headers["retry-after"]).toBe("31");
      // The terminator is upstream backpressure, not the gateway clock.
      expect(res.headers["x-gateway-error-class"]).not.toBe("gateway-fault");
      expect(calls).toBe(1); // retry skipped → no second billable attempt
      // A budget-skip is not failure evidence — the breaker must not count it.
      expect(recorded).toEqual(["INCONCLUSIVE"]);

      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        attempts: 1,
        retry_disposition: "skipped_budget",
      });
    } finally {
      await app.close();
    }
  });

  it("8.43 status-agnostic — 429 budget-skip: Retry-After beyond the wall-clock → passthrough 429, single attempt, breaker not incremented", async () => {
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    const body429 =
      '{"error":{"message":"rate limited","type":"rate_limit_error"}}';
    let calls = 0;
    // A second budget-skip status the 503 case can't witness: pins that the
    // policy passes the upstream status through verbatim, not a fixed value.
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve({
        kind: "upstream_error",
        status: 429,
        body_raw: body429,
        retry_after: "31",
      });
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(429); // verbatim upstream status, not a fixed value
      expect(res.payload).toBe(body429);
      expect(res.headers["retry-after"]).toBe("31");
      expect(res.headers["x-gateway-error-class"]).not.toBe("gateway-fault");
      expect(calls).toBe(1);
      expect(recorded).toEqual(["INCONCLUSIVE"]);

      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(1);
      expect(complete[0]).toMatchObject({
        attempts: 1,
        retry_disposition: "skipped_budget",
      });
    } finally {
      await app.close();
    }
  });

  it("8.43 sibling — Retry-After within budget retries: 503 + Retry-After:1 then 200 → 200, two attempts", async () => {
    let calls = 0;
    const okBody = { id: "chatcmpl-8-43-fits", choices: [{ index: 0 }] };
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();
    // Same 503 + Retry-After path as the budget-skip above, but a wait that FITS
    // the budget, so the retry fires — isolating the boundary as the difference.
    const upstream = (): Promise<Outcome> => {
      calls += 1;
      return Promise.resolve(
        calls === 1
          ? {
              kind: "upstream_error",
              status: 503,
              body_raw: '{"error":{"message":"overloaded"}}',
              retry_after: "1",
            }
          : { kind: "ok", status: 200, body_parsed: okBody },
      );
    };
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstream,
      capture,
    );
    // Fake timers drive the 1s Retry-After wait deterministically (app booted on
    // real timers).
    vi.useFakeTimers();
    try {
      const injected = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      await vi.advanceTimersByTimeAsync(1000); // honor the 1s Retry-After
      const res = await injected;
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.payload)).toEqual(okBody);
      expect(calls).toBe(2); // the 1s wait fit the budget → retry fired
      expect(recorded).toEqual(["SUCCESS"]); // 503-then-200 is a success
      expect(capture.byEvent("req_complete")[0]).toMatchObject({
        attempts: 2,
        retry_disposition: "attempted",
      });
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });

  it("8.24 hook ordering: authPreHandler → rate-limiter preHandler → proxy handler", async () => {
    const order: string[] = [];
    let tenantIdAtRateLimiter: string | null = null;
    const tenantId = randomUUID();
    const upstream = (): Promise<Outcome> => {
      order.push("proxy");
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    // Inject the rate-limiter preHandler exactly where a future change would,
    // via registerProtected — no production app.ts wiring is touched.
    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        scope.addHook("preHandler", async (request) => {
          order.push("rate");
          // tenantId is populated by authPreHandler (onRequest); its presence
          // here proves auth ran before this preHandler.
          tenantIdAtRateLimiter = request.tenantId;
        });
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: upstream,
        });
      },
    });
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      expect(res.statusCode).toBe(200);
      expect(order).toEqual(["rate", "proxy"]); // preHandler before the route handler
      expect(tenantIdAtRateLimiter).toBe(tenantId); // onRequest auth before the preHandler
    } finally {
      await app.close();
    }
  });

  it("8.40 unrecognized rejection (EHOSTUNREACH) -> 500 gateway-fault, breaker INCONCLUSIVE, boundary log abort_identity:false", async () => {
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();

    // The fake stands in for the real client at the upstreamBuffered seam. To
    // exercise the recognition + re-throw under test (and NOT pre-bake an
    // Outcome that bypasses it), it must run an unrecognized rejection through
    // resolveRejection. The route hands it (body, signal, log); resolveRejection
    // is already imported.
    const upstreamUnrecognized = (
      _body: ChatCompletionsBody,
      signal: AbortSignal,
      log: Logger,
    ): Promise<Outcome> => {
      return Promise.reject(fetchFailed("EHOSTUNREACH")).catch((err: unknown) =>
        resolveRejection(err, signal, log),
      );
    };

    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstreamUnrecognized,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });

      // Status + breaker are identical for BOTH 8.40 variants (true/false), so
      // they pin the route wiring but cannot tell the variants apart.
      expect(res.statusCode).toBe(500);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(JSON.parse(res.payload)).toEqual({
        error: {
          message: "internal server error",
          type: "internal_error",
          code: "unhandled_exception",
        },
      });
      expect(recorded).toEqual(["INCONCLUSIVE"]);

      // The discriminant. The boundary log is a plain `error` log (no `event`
      // field), so it is read from `capture.logs`, not `byEvent`.
      const boundary = capture.logs.find(
        (line) => line.msg === "Unrecognized upstream rejection",
      );
      expect(boundary).toMatchObject({
        abort_identity: false,
        cause_code: "EHOSTUNREACH",
      });
    } finally {
      await app.close();
    }
  });

  it("8.40 unrecognized rejection (out-of-contract abort) -> 500 gateway-fault, breaker INCONCLUSIVE, boundary log abort_identity:true", async () => {
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();

    // Sibling of the EHOSTUNREACH case — the opposite arm of the discriminant.
    // The route's real signal can never carry an unrecognized reason (single
    // abort point: only the gateway's funnel aborts it, always with a recognized
    // kind), so the fake must SYNTHESIZE the out-of-contract abort with its own
    // controller, then run that reason through resolveRejection.
    const upstreamOutOfContractAbort = (
      _body: ChatCompletionsBody,
      _signal: AbortSignal,
      log: Logger,
    ): Promise<Outcome> => {
      const controller = new AbortController();
      controller.abort();
      return Promise.reject(controller.signal.reason).catch((err: unknown) =>
        resolveRejection(err, controller.signal, log),
      );
    };

    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstreamOutOfContractAbort,
      capture,
    );
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });

      // Identical to the EHOSTUNREACH case — the client cannot tell the two
      // apart; only the boundary log can.
      expect(res.statusCode).toBe(500);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(recorded).toEqual(["INCONCLUSIVE"]);

      // The discriminant: same destination, but abort_identity is TRUE (the
      // rejection IS the gateway's own abort reason) and the rejection is an
      // AbortError — the out-of-contract aborter, not an exotic network error.
      const boundary = capture.logs.find(
        (line) => line.msg === "Unrecognized upstream rejection",
      );
      expect(boundary).toMatchObject({
        abort_identity: true,
        err_name: "AbortError",
      });
    } finally {
      await app.close();
    }
  });

  it("8.39 settle-once race: a recognized network rejection (ECONNRESET) lands as the wall-clock abort fires → the route honors the returned network_failed (504 upstream_connection_failed + boundary log), not a signal.aborted shortcut", async () => {
    const recorded: ProbeOutcome[] = [];
    const capture = makeLogCapture();

    const upstreamRace = (
      _body: ChatCompletionsBody,
      signal: AbortSignal,
      log: Logger,
    ): Promise<Outcome> => {
      return new Promise<Outcome>((resolve) => {
        signal.addEventListener(
          "abort",
          () =>
            resolve(resolveRejection(fetchFailed("ECONNRESET"), signal, log)),
          { once: true },
        );
      });
    };

    // Build under real timers, then drive the 30s wall-clock abort with fake timers.
    const { app, apiKey } = await buildWith(
      recordingBreaker(recorded),
      upstreamRace,
      capture,
    );
    vi.useFakeTimers();
    try {
      const injected = app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: `Bearer ${apiKey}` },
        payload: validBody,
      });
      await vi.advanceTimersByTimeAsync(30_000);
      const res = await injected;

      // Dead-weight: a signal.aborted shortcut also yields 504/gateway-fault/FAILURE.
      expect(res.statusCode).toBe(504);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(recorded).toEqual(["FAILURE"]);

      // Discriminant: the returned network_failed body; a shortcut would render
      // the wall_clock_exceeded body instead.
      expect(JSON.parse(res.payload)).toEqual({
        error: {
          message: "gateway error",
          type: "gateway_error",
          code: "upstream_connection_failed",
        },
      });

      // Discriminant: classify() logs the network cause but is a NO-OP for aborted,
      // so a shortcut emits no boundary log. The cause-log has no msg/event — match
      // by cause_code.
      const boundary = capture.logs.find(
        (line) => line.cause_code === "ECONNRESET",
      );
      expect(boundary).toMatchObject({
        cause_code: "ECONNRESET",
        cause_name: "Error",
      });
    } finally {
      vi.useRealTimers();
      await app.close();
    }
  });
});
