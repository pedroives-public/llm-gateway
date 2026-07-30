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
// first blocked redirect is guaranteed to be the "first occurrence". Inside
// the sibling file (proxy-upstream-redirect.test.ts) earlier redirect cells
// would already have consumed the alert flag, making the assertion
// order-dependent.

describe("upstream redirect blocked — first-occurrence operator alert", () => {
  it("two blocked redirects → two req_complete records but exactly ONE operational_alert", async () => {
    const upstreamRedirect = (): Promise<Outcome> =>
      Promise.resolve({ kind: "redirect_blocked" });
    const capture = makeLogCapture();

    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(randomUUID()),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: upstreamRedirect,
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
        alert: "upstream_redirect_blocked",
        req_id: complete[0]?.req_id,
      });
    } finally {
      await app.close();
    }
  });
});
