import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute, type ProxyRouteOptions } from "../../src/routes/proxy.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { makeLogCapture, type LogCapture } from "../log-capture.js";

// Cost-cap policy: max_tokens, max_completion_tokens and n are the
// client-controlled multipliers of upstream spend, so the schema bounds them —
// a single request must not be able to command unbounded output cost. Cap
// rejections are deliberate gateway policy, not shape errors: they answer 400
// with a field-specific code (same pattern as stream_not_supported).
const MAX_OUTPUT_TOKENS_CAP = 16_384;

describe("cost caps on client-controlled spend fields", () => {
  async function buildCapProbe(
    upstreamBuffered?: ProxyRouteOptions["upstreamBuffered"],
  ): Promise<{ app: FastifyInstance; capture: LogCapture }> {
    const capture = makeLogCapture();
    const app = await buildApp({
      logger: capture.logger,
      db: fakeAuthDb(randomUUID()),
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered:
            upstreamBuffered ??
            (async () => {
              throw new Error(
                "tripwire: upstream reached — request passed the cap",
              );
            }),
        });
      },
    });
    return { app, capture };
  }

  it("max_tokens above the cap -> 400 max_tokens_too_large, upstream untouched", async () => {
    const { app } = await buildCapProbe();

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: JSON.stringify({
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
          max_tokens: MAX_OUTPUT_TOKENS_CAP + 1,
        }),
      });

      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(res.json()).toMatchObject({
        error: {
          type: "invalid_request_error",
          code: "max_tokens_too_large",
        },
      });
    } finally {
      await app.close();
    }
  });

  // Modern SDKs send max_completion_tokens instead of max_tokens; with
  // additionalProperties:true an uncapped alias would travel to the upstream
  // verbatim and bypass the ceiling entirely, so both fields carry the same cap.
  it("max_completion_tokens above the cap -> 400 max_completion_tokens_too_large", async () => {
    const { app } = await buildCapProbe();

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: JSON.stringify({
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
          max_completion_tokens: MAX_OUTPUT_TOKENS_CAP + 1,
        }),
      });

      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(res.json()).toMatchObject({
        error: {
          type: "invalid_request_error",
          code: "max_completion_tokens_too_large",
        },
      });
    } finally {
      await app.close();
    }
  });

  // n multiplies the whole output cost (n choices = n completions billed), so
  // V1 pins it to 1 outright rather than capping a multiplier no client of the
  // gateway needs — same "not supported" posture as stream:true.
  it("n above 1 -> 400 n_not_supported", async () => {
    const { app } = await buildCapProbe();

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: JSON.stringify({
          model: "gpt-4o",
          messages: [{ role: "user", content: "hi" }],
          n: 2,
        }),
      });

      expect(res.statusCode).toBe(400);
      expect(res.headers["x-gateway-error-class"]).toBe("client-fault");
      expect(res.json()).toMatchObject({
        error: {
          type: "invalid_request_error",
          code: "n_not_supported",
        },
      });
    } finally {
      await app.close();
    }
  });

  // Observability: cost-cap rejections are the attack/demand signal the caps
  // exist to measure, so they carry their own req_rejected reason instead of
  // blending into the schema_validation population. stream:true is policy but
  // not cost policy — it must stay schema_validation (population boundary pin).
  it("cap rejections emit reason cost_cap_exceeded; stream stays schema_validation", async () => {
    async function rejectionReason(body: Record<string, unknown>) {
      const { app, capture } = await buildCapProbe();
      try {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: {
            authorization: bearer(),
            "content-type": "application/json",
          },
          payload: JSON.stringify({
            model: "gpt-4o",
            messages: [{ role: "user", content: "hi" }],
            ...body,
          }),
        });
        expect(res.statusCode).toBe(400);
        const events = capture.byEvent("req_rejected");
        expect(events).toHaveLength(1);
        return events[0]?.reason;
      } finally {
        await app.close();
      }
    }

    expect(await rejectionReason({ max_tokens: MAX_OUTPUT_TOKENS_CAP + 1 })).toBe(
      "cost_cap_exceeded",
    );
    expect(
      await rejectionReason({ max_completion_tokens: MAX_OUTPUT_TOKENS_CAP + 1 }),
    ).toBe("cost_cap_exceeded");
    expect(await rejectionReason({ n: 2 })).toBe("cost_cap_exceeded");
    expect(await rejectionReason({ stream: true })).toBe("schema_validation");
  });
});
