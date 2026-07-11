import { randomBytes, randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { describe, it, expect } from "vitest";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import {
  createCircuitBreaker,
  type CircuitBreaker,
} from "../../src/reliability/circuit-breaker.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import type { DrizzleClient } from "../../src/db/client.js";

// One client must not open the process-wide circuit breaker for everyone else
// by sending `stream: true`: a streaming upstream answers it 200 + SSE, which
// the buffered client cannot JSON-parse -> `undecodable` -> breaker FAILURE.
// Five of those open the shared breaker and every other client gets 503.
//
// Scope: proves this ONE client-controllable inducer is closed — a genuine
// upstream 5xx must still open the breaker for everyone; that is its job.
//
// "200 non-JSON body -> undecodable" is already pinned at the client level in
// tests/upstream/openai.test.ts; the first test here proves the composition
// through the real route, client, and breaker — do not delete that unit test.

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};

// The fake upstream's reply to a non-stream request. The victim asserts this
// body verbatim — a liveness check a dead fake cannot satisfy (post-fix the
// attackers never reach the fake at all).
const VICTIM_BODY = {
  id: "chatcmpl-victim",
  choices: [{ message: { role: "assistant", content: "served" } }],
};

// One active row for any presented key, so an authenticated request reaches the
// route. Auth edge cases live in tests/middleware/auth.test.ts. Same shape as
// proxy.test.ts / proxy-composition.test.ts — intentionally duplicated.
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

const bearer = (): string =>
  `Bearer lkey_${randomBytes(32).toString("base64url")}`;

// The breaker emits cb_state_change through a logger it never reads back here.
const noopLog = { info: () => {} };

describe("stream:true must not open the shared circuit breaker", () => {
  it(
    "stream:true (SSE-shaped 200) does not open the shared breaker for another client",
    { timeout: 30_000 },
    async () => {
      // >= FAILURE_LIMIT (5) with margin; the exact threshold is pinned in
      // tests/reliability/circuit-breaker.test.ts, so N need only cross it.
      const ATTACKER_REQUESTS = 7;

      // Fake upstream: a real streaming provider answers `stream: true` with an
      // SSE body (unparseable as JSON) and a normal request with JSON. It MUST
      // end each response — a held-open stream would block the reader to the
      // wall-clock and open the breaker for the wrong vector.
      const server = http.createServer((req, res) => {
        res.on("error", () => {});
        let raw = "";
        req.on("data", (chunk) => {
          raw += chunk;
        });
        req.on("end", () => {
          let streamRequested = false;
          try {
            streamRequested =
              (JSON.parse(raw) as { stream?: unknown }).stream === true;
          } catch {
            streamRequested = false;
          }
          if (streamRequested) {
            res.writeHead(200, { "content-type": "text/event-stream" });
            res.end(
              'data: {"id":"chatcmpl-x","choices":[{"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n',
            );
          } else {
            res.writeHead(200, { "content-type": "application/json" });
            res.end(JSON.stringify(VICTIM_BODY));
          }
        });
      });
      await new Promise<void>((resolve) =>
        server.listen(0, "127.0.0.1", resolve),
      );
      const { port } = server.address() as AddressInfo;

      // Real breaker + real client (pointed at the fake) wired through the real
      // route. Holding the breaker lets us read its state directly; injecting
      // the client avoids mutating OPENAI_BASE_URL.
      const heldBreaker = createCircuitBreaker(noopLog);
      const client = createOpenAIClient({
        apiKey: "gateway-key",
        baseURL: `http://127.0.0.1:${port}`,
      });
      const app = await buildApp({
        logger: false,
        db: fakeAuthDb(randomUUID()),
        registerProtected: async (scope) => {
          await scope.register(proxyRoute, {
            breaker: heldBreaker,
            upstreamBuffered: client.buffered,
          });
        },
      });

      const attackerAuth = bearer();
      const victimAuth = bearer();

      try {
        // Attacker: one client hammers `stream: true`, serially (parallel would
        // race the 5th FAILURE against the victim's admission).
        for (let i = 0; i < ATTACKER_REQUESTS; i++) {
          await app.inject({
            method: "POST",
            url: "/v1/chat/completions",
            headers: { authorization: attackerAuth },
            payload: { ...validBody, stream: true },
          });
        }

        // Victim: a second, ordinary client (stream absent). In V1 the breaker
        // is process-global, so the victim's identity is immaterial to the
        // wedge; the distinct key documents "another client".
        const victim = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: victimAuth },
          payload: validBody,
        });

        expect(victim.statusCode).toBe(200);
        expect(victim.json()).toStrictEqual(VICTIM_BODY);
        expect(heldBreaker.getState()).toBe("CLOSED");
      } finally {
        await app.close();
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    },
  );

  it("rejects stream:true before the upstream or the breaker is touched", async () => {
    // Mechanism (era-pinned to pre-streaming): the fix must fire at the schema,
    // before any upstream call or breaker record. Rework when streaming ships.
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    // Both breaker members are tripwires: a correct fix rejects the request
    // before the breaker is consulted (tryAcquire) or updated (recordResult),
    // so any call to either is a visible failure.
    const tripwireBreaker: CircuitBreaker = {
      tryAcquire: () => {
        throw new Error(
          "tryAcquire must not run: stream:true must be rejected before the breaker is consulted",
        );
      },
      recordResult: () => {
        throw new Error(
          "recordResult must not run: stream:true must be rejected before the breaker",
        );
      },
      getState: () => "CLOSED",
    };
    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(randomUUID()),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: tripwireBreaker,
          upstreamBuffered: countingUpstream,
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: true },
      });

      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(res.json()).toMatchObject({
        error: { code: "stream_not_supported" },
      });
      expect(upstreamCalls).toBe(0);
    } finally {
      await app.close();
    }
  });

  it("still accepts stream:false and an absent stream field (no over-block)", async () => {
    // Negative control: the fix must reject only `true`, never the field. Green
    // today and post-correct-fix; RED only against an over-broad fix.
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({
        kind: "ok",
        status: 200,
        body_parsed: VICTIM_BODY,
      });
    };
    const okBreaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: () => {},
      getState: () => "CLOSED",
    };
    const app = await buildApp({
      logger: false,
      db: fakeAuthDb(randomUUID()),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: okBreaker,
          upstreamBuffered: countingUpstream,
        });
      },
    });

    try {
      const explicitFalse = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: { ...validBody, stream: false },
      });
      const absent = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer() },
        payload: validBody,
      });

      expect(explicitFalse.statusCode).toBe(200);
      expect(absent.statusCode).toBe(200);
      expect(upstreamCalls).toBe(2); // both reached the upstream
    } finally {
      await app.close();
    }
  });
});
