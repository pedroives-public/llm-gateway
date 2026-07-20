import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import Fastify, { type FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute, type ProxyRouteOptions } from "../../src/routes/proxy.js";
import { wasColdStart } from "../../src/observability/events.js";
import type { DrizzleClient } from "../../src/db/client.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { makeLogCapture, type LogCapture } from "../log-capture.js";

// Schema rejections end the request before the handler runs, so they get their
// own terminal observability event (`req_rejected`) instead of borrowing the
// handler lifecycle: exactly one req_rejected, never req_start/req_complete.
// This keeps the SLI population (handler work) separate from rejection telemetry.
describe("req_rejected terminal event for schema rejections", () => {
  async function buildRejectionProbe(
    tenantId: string,
    upstreamBuffered?: ProxyRouteOptions["upstreamBuffered"],
  ): Promise<{ app: FastifyInstance; capture: LogCapture }> {
    const capture = makeLogCapture();
    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(tenantId),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered:
            upstreamBuffered ??
            (async () => {
              throw new Error(
                "tripwire: upstream reached — request passed validation",
              );
            }),
        });
      },
    });
    return { app, capture };
  }

  it("schema-invalid body -> 400 + exactly one req_rejected, no lifecycle events", async () => {
    const { app, capture } = await buildRejectionProbe(randomUUID());

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: '{"model": "gpt-4o"}',
      });

      expect(res.statusCode).toBe(400);
      expect(capture.byEvent("req_rejected")).toHaveLength(1);
      expect(capture.byEvent("req_start")).toHaveLength(0);
      expect(capture.byEvent("req_complete")).toHaveLength(0);
    } finally {
      await app.close();
    }
  });

  // The probe URL carries a query string on purpose: the route pin must hold
  // the registered route pattern, never the client-influenced raw request URL.
  it("req_rejected payload carries exactly the allowlisted contract fields", async () => {
    const tenantId = randomUUID();
    const auth = bearer();
    const { app, capture } = await buildRejectionProbe(tenantId);

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions?probe=1",
        headers: {
          authorization: auth,
          "content-type": "application/json",
        },
        payload: '{"model": "gpt-4o"}',
      });

      expect(res.statusCode).toBe(400);
      const events = capture.byEvent("req_rejected");
      expect(events).toHaveLength(1);
      const evt = events[0] ?? {};

      expect(evt).toMatchObject({
        event: "req_rejected",
        tenant_id: tenantId,
        route: "/v1/chat/completions",
        reason: "schema_validation",
        status: 400,
      });
      expect(typeof evt.req_id).toBe("string");
      expect(evt.req_id).not.toBe("");

      // Allowlist pin: nothing beyond contract fields + Pino/Fastify infra.
      const INFRA_KEYS = ["level", "time", "pid", "hostname", "reqId"];
      const CONTRACT_KEYS = [
        "event",
        "req_id",
        "tenant_id",
        "route",
        "reason",
        "status",
      ];
      const unexpected = Object.keys(evt).filter(
        (key) => !INFRA_KEYS.includes(key) && !CONTRACT_KEYS.includes(key),
      );
      expect(unexpected).toEqual([]);

      // Value-level leak pins: client-influenced text must not ride inside
      // allowed fields either.
      const serialized = JSON.stringify(evt);
      expect(serialized).not.toContain("must have required property");
      expect(serialized).not.toContain(auth.slice("Bearer ".length));
      expect(serialized).not.toContain("probe=1");
    } finally {
      await app.close();
    }
  });

  // Hybrid discriminator: a schema rejection is recognized only when BOTH
  // signals agree — the FST_ERR_VALIDATION code AND a populated `validation`
  // array. A lone signal is an impossible state for this desk: it must fall
  // to the unhandled-fault branch (500 + anomaly log), never emit req_rejected.
  it("validation array without FST_ERR_VALIDATION code -> 500 anomaly, no req_rejected", async () => {
    const impostor = Object.assign(new Error("impostor upstream failure"), {
      code: "E_IMPOSTOR",
      validation: [{ keyword: "impostor" }],
    });
    const { app, capture } = await buildRejectionProbe(
      randomUUID(),
      async () => {
        throw impostor;
      },
    );

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload:
          '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}',
      });

      expect(res.statusCode).toBe(500);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(capture.byEvent("req_rejected")).toHaveLength(0);
      expect(
        capture.logs.some(
          (line) => line.level === 50 && typeof line.req_id === "string",
        ),
      ).toBe(true);
    } finally {
      await app.close();
    }
  });

  it("FST_ERR_VALIDATION code without validation array -> 500 anomaly, no req_rejected", async () => {
    const halfSignal = Object.assign(
      new Error("code without validation array"),
      { code: "FST_ERR_VALIDATION" },
    );
    const { app, capture } = await buildRejectionProbe(
      randomUUID(),
      async () => {
        throw halfSignal;
      },
    );

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload:
          '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}',
      });

      expect(res.statusCode).toBe(500);
      expect(res.headers["x-gateway-error-class"]).toBe("gateway-fault");
      expect(capture.byEvent("req_rejected")).toHaveLength(0);
      expect(
        capture.logs.some(
          (line) => line.level === 50 && typeof line.req_id === "string",
        ),
      ).toBe(true);
    } finally {
      await app.close();
    }
  });

  // Ordering constraint: the next two cells must run before any cell in this
  // file that emits req_complete — cold-start state is module-global and only
  // a completed request may consume it.
  it("a rejection leaves the cold-start marker unconsumed", async () => {
    expect(wasColdStart()).toBe(true);
    const { app, capture } = await buildRejectionProbe(randomUUID());

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: '{"model": "gpt-4o"}',
      });

      expect(res.statusCode).toBe(400);
      expect(capture.byEvent("req_rejected")).toHaveLength(1);
      expect(wasColdStart()).toBe(true);
    } finally {
      await app.close();
    }
  });

  it("valid body -> 200, one req_start + req_complete, no req_rejected, still cold", async () => {
    const { app, capture } = await buildRejectionProbe(
      randomUUID(),
      async () => ({
        kind: "ok" as const,
        status: 200,
        body_parsed: { id: "cmpl-ok" },
      }),
    );

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload:
          '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}',
      });

      expect(res.statusCode).toBe(200);
      expect(capture.byEvent("req_start")).toHaveLength(1);
      expect(capture.byEvent("req_complete")).toHaveLength(1);
      expect(capture.byEvent("req_rejected")).toHaveLength(0);
      expect(capture.byEvent("req_start")[0]).toMatchObject({
        was_cold_start: true,
      });
    } finally {
      await app.close();
    }
  });

  it("413 body-too-large -> request_too_large, no req_rejected, no lifecycle events", async () => {
    const { app, capture } = await buildRejectionProbe(randomUUID());

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: `{"model":"gpt-4o","messages":[{"role":"user","content":"${"x".repeat(262_200)}"}]}`,
      });

      expect(res.statusCode).toBe(413);
      expect(res.json()).toMatchObject({
        error: { code: "request_too_large" },
      });
      expect(capture.byEvent("req_rejected")).toHaveLength(0);
      expect(capture.byEvent("req_start")).toHaveLength(0);
    } finally {
      await app.close();
    }
  });
});

// A schema rejection with a null tenant means the route was registered without
// the auth hook — a gateway misconfiguration, not client traffic. The event's
// population is authenticated-only, so the desk must not emit req_rejected;
// it must log an operator-facing anomaly instead of skipping silently.
describe("req_rejected population guard: tenant-null anomaly", () => {
  it("validation failure without auth -> 400, no req_rejected, one anomaly log", async () => {
    const capture = makeLogCapture();
    const app = Fastify({ logger: capture.logger });
    app.decorateRequest("tenantId", null);
    app.decorateRequest("planTier", null);
    app.decorateRequest("reqId", "");
    app.addHook("onRequest", async (request) => {
      request.reqId = randomUUID();
    });
    await app.register(proxyRoute, {
      breaker: stubBreaker,
      upstreamBuffered: async () => {
        throw new Error(
          "tripwire: upstream reached — request passed validation",
        );
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { "content-type": "application/json" },
        payload: '{"model": "gpt-4o"}',
      });

      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(capture.byEvent("req_rejected")).toHaveLength(0);

      const anomalies = capture.logs.filter((line) => line.level === 50);
      expect(anomalies.length).toBeGreaterThanOrEqual(1);
      expect(anomalies.some((line) => typeof line.req_id === "string")).toBe(
        true,
      );
      expect(JSON.stringify(anomalies)).not.toContain(
        "must have required property",
      );
    } finally {
      await app.close();
    }
  });
});

// Auth runs at onRequest, before parsing and validation: a request failing
// both auth and schema is answered 401 with no events — the lifecycle order
// that makes the authenticated-only req_rejected population possible.
describe("lifecycle order: auth precedes validation", () => {
  it("invalid key + invalid body -> 401, no req_rejected, no req_start", async () => {
    const emptyAuthDb = {
      select() {
        return {
          from() {
            return {
              innerJoin() {
                return {
                  where() {
                    return {
                      limit() {
                        return Promise.resolve([]);
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

    const capture = makeLogCapture();
    const app = await buildApp({
      logger: capture.logger,
      db: emptyAuthDb,
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: async () => {
            throw new Error(
              "tripwire: upstream reached — request passed validation",
            );
          },
        });
      },
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: '{"model": "gpt-4o"}',
      });

      expect(res.statusCode).toBe(401);
      expect(capture.byEvent("req_rejected")).toHaveLength(0);
      expect(capture.byEvent("req_start")).toHaveLength(0);
    } finally {
      await app.close();
    }
  });
});
