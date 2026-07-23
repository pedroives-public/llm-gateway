import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { makeLogCapture } from "../log-capture.js";

// Lives in its own file on purpose: the operator alert fires once per process,
// and Vitest gives each test FILE a fresh module registry — so this file's
// first 401 is guaranteed to be the "first occurrence". Inside the sibling
// file (proxy-upstream-auth.test.ts) earlier 401 cells would already have
// consumed the alert flag, making the exactly-once assertion order-dependent.

describe("upstream 401 — first-occurrence operator alert", () => {
  it("two 401 requests → two req_complete records but exactly ONE operational_alert", async () => {
    const upstream401 = (): Promise<Outcome> =>
      Promise.resolve({
        kind: "upstream_error",
        status: 401,
        body_raw: '{"error":{"message":"Incorrect API key"}}',
      });
    const capture = makeLogCapture();

    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(randomUUID()),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: upstream401,
        });
      },
    });

    try {
      for (let i = 1; i <= 2; i++) {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: {
            model: "gpt-4o",
            messages: [{ role: "user", content: "hi" }],
          },
        });
        expect(res.statusCode, `request ${i}`).toBe(502);
      }

      // Events describe: every failed request is recorded.
      const complete = capture.byEvent("req_complete");
      expect(complete).toHaveLength(2);

      // Alerts summon: only the first occurrence pages the operator, and it
      // correlates to the request that crossed the healthy→broken line.
      const alerts = capture.byEvent("operational_alert");
      expect(alerts).toHaveLength(1);
      expect(alerts[0]).toMatchObject({
        alert: "upstream_auth_failure",
        req_id: complete[0]?.req_id,
      });
    } finally {
      await app.close();
    }
  });
});
