import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import Fastify, { type FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
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
  ): Promise<{ app: FastifyInstance; capture: LogCapture }> {
    const capture = makeLogCapture();
    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(tenantId),
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
