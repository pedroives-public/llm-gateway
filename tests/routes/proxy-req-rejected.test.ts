import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { makeLogCapture } from "../log-capture.js";

// Schema rejections end the request before the handler runs, so they get their
// own terminal observability event (`req_rejected`) instead of borrowing the
// handler lifecycle: exactly one req_rejected, never req_start/req_complete.
// This keeps the SLI population (handler work) separate from rejection telemetry.
describe("req_rejected terminal event for schema rejections", () => {
  it("schema-invalid body -> 400 + exactly one req_rejected, no lifecycle events", async () => {
    const capture = makeLogCapture();
    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(randomUUID()),
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

      expect(res.statusCode).toBe(400);
      expect(capture.byEvent("req_rejected")).toHaveLength(1);
      expect(capture.byEvent("req_start")).toHaveLength(0);
      expect(capture.byEvent("req_complete")).toHaveLength(0);
    } finally {
      await app.close();
    }
  });
});
