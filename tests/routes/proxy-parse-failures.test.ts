import { describe, expect, it } from "vitest";
import type { FastifyError, FastifyInstance } from "fastify";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { bearer } from "../helpers/fake-auth-db.js";

// Pre-validation parse failures at the proxy error desk. This suite is being
// converted from probe to contract via TDD: tests asserting the target
// contract (4xx client-fault with a public code) go first; tests labeled
// OBSERVED still pin today's mislabeled behavior (500 gateway-fault) until
// their slice flips them. The DIAGNOSTIC test is temporary scaffolding for
// picking the error discriminator and is removed when the desk is done.
describe("probe: pre-validation parse failures at the proxy error desk", () => {
  function probeApp(): Promise<FastifyInstance> {
    return buildProxyApp({
      breaker: stubBreaker,
      upstreamBuffered: async () => {
        throw new Error("tripwire: upstream reached — request passed parsing");
      },
    });
  }

  async function probe(
    contentType: string,
    payload: string,
  ): Promise<{ status: number; errorClass: unknown; body: unknown }> {
    const app = await probeApp();
    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: { authorization: bearer(), "content-type": contentType },
        payload,
      });
      return {
        status: res.statusCode,
        errorClass: res.headers["x-gateway-error-class"],
        body: res.json(),
      };
    } finally {
      await app.close();
    }
  }

  it("malformed JSON body -> 400 client-fault malformed_json", async () => {
    const observed = await probe("application/json", '{"model": "gpt-4o", ');
    expect(observed).toMatchObject({
      status: 400,
      errorClass: "client-fault",
      body: {
        error: { type: "invalid_request_error", code: "malformed_json" },
      },
    });
  });

  it("OBSERVED: text/plain is parsed natively; string body fails schema -> 400 client-fault", async () => {
    const observed = await probe("text/plain", "model=gpt-4o");
    expect(observed).toMatchObject({
      status: 400,
      errorClass: "client-fault",
    });
  });

  it("OBSERVED: unparseable content-type -> 500 gateway-fault (mislabeled client fault)", async () => {
    const observed = await probe("application/xml", "<model/>");
    expect(observed).toMatchObject({
      status: 500,
      errorClass: "gateway-fault",
    });
  });

  it("empty JSON body -> 400 client-fault empty_body", async () => {
    const observed = await probe("application/json", "");
    expect(observed).toMatchObject({
      status: 400,
      errorClass: "client-fault",
      body: {
        error: { type: "invalid_request_error", code: "empty_body" },
      },
    });
  });

  it("control: schema-invalid JSON stays a 400 client-fault", async () => {
    const observed = await probe("application/json", '{"model": "gpt-4o"}');
    expect(observed).toMatchObject({
      status: 400,
      errorClass: "client-fault",
      body: { error: { code: "messages_missing" } },
    });
  });

  it("DIAGNOSTIC (temporary): raw Fastify error properties per parse-failure case", async () => {
    const Fastify = (await import("fastify")).default;
    const app = Fastify({ logger: false });
    const seen: Record<string, unknown>[] = [];
    app.setErrorHandler<FastifyError>((error, _request, reply) => {
      seen.push({
        name: error.name,
        code: error.code,
        statusCode: error.statusCode,
        hasValidation: error.validation !== undefined,
      });
      reply.code(599).send({});
    });
    app.post("/x", { schema: { body: { type: "object" } } }, async () => ({}));
    const cases: ReadonlyArray<readonly [string, string]> = [
      ["application/json", '{"a": '],
      ["application/json", ""],
      ["application/xml", "<x/>"],
    ];
    try {
      for (const [contentType, payload] of cases) {
        await app.inject({
          method: "POST",
          url: "/x",
          headers: { "content-type": contentType },
          payload,
        });
      }
    } finally {
      await app.close();
    }
    const { writeFileSync } = await import("node:fs");
    writeFileSync(
      "/tmp/claude-1000/-home-pedro-projects-llm-gateway/cc21e71c-5814-4870-b9ed-b3ff0fd64631/scratchpad/diag-parse-errors.json",
      JSON.stringify(seen, null, 2),
    );
    expect(seen).toHaveLength(3);
  });
});
