import http from "node:http";
import { describe, it, expect } from "vitest";
import {
  createCircuitBreaker,
  type CircuitBreaker,
} from "../../src/reliability/circuit-breaker.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { buildProxyApp } from "../helpers/proxy-app.js";

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
          let streamRequested: boolean;
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
      const { port, close } = await listenEphemeral(server);

      // Real breaker + real client (pointed at the fake) wired through the real
      // route. Holding the breaker lets us read its state directly; injecting
      // the client avoids mutating OPENAI_BASE_URL.
      const heldBreaker = createCircuitBreaker(noopLog);
      const client = createOpenAIClient({
        apiKey: "gateway-key",
        baseURL: `http://127.0.0.1:${port}`,
      });
      const app = await buildProxyApp({
        breaker: heldBreaker,
        upstreamBuffered: client.buffered,
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
        await close();
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
    const app = await buildProxyApp({
      breaker: tripwireBreaker,
      upstreamBuffered: countingUpstream,
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
    const app = await buildProxyApp({
      breaker: stubBreaker,
      upstreamBuffered: countingUpstream,
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

describe("stream field under Ajv default type coercion", () => {
  // Fastify's default Ajv coerces body types, so the `const: false` pin is
  // also reachable through coercible values: "true"/1 coerce to true and must
  // be rejected exactly like the boolean; "false"/0 coerce to false and must
  // stay accepted. A validator config change that turns coercion off would
  // flip these outcomes — these pins make that drift loud.
  it("rejects 'true' and 1 as stream:true, before any upstream call", async () => {
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    const app = await buildProxyApp({
      breaker: stubBreaker,
      upstreamBuffered: countingUpstream,
    });

    try {
      for (const coerced of ["true", 1]) {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: coerced },
        });
        expect(res.statusCode, `stream: ${JSON.stringify(coerced)}`).toBe(400);
        expect(res.json()).toMatchObject({
          error: { code: "stream_not_supported" },
        });
      }
      expect(upstreamCalls).toBe(0);
    } finally {
      await app.close();
    }
  });

  it("accepts 'false' and 0 as stream:false (no over-block)", async () => {
    let upstreamCalls = 0;
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };
    const app = await buildProxyApp({
      breaker: stubBreaker,
      upstreamBuffered: countingUpstream,
    });

    try {
      for (const coerced of ["false", 0]) {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: { ...validBody, stream: coerced },
        });
        expect(res.statusCode, `stream: ${JSON.stringify(coerced)}`).toBe(200);
      }
      expect(upstreamCalls).toBe(2);
    } finally {
      await app.close();
    }
  });
});
