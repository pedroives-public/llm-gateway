import { describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { bearer } from "../helpers/fake-auth-db.js";

// Pre-validation parse failures must be answered as client faults with a
// stable public error code, never fall through to the unhandled 500 branch.
// Exception: Fastify parses text/plain natively, so that string body is
// rejected by schema validation, not by a parse branch of the desk.
describe("pre-validation parse failures at the proxy error desk", () => {
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

  it("text/plain is parsed natively; string body fails schema -> 400 client-fault", async () => {
    const observed = await probe("text/plain", "model=gpt-4o");
    expect(observed).toMatchObject({
      status: 400,
      errorClass: "client-fault",
    });
  });

  it("unparseable content-type -> 415 client-fault unsupported_content_type", async () => {
    const observed = await probe("application/xml", "<model/>");
    expect(observed).toMatchObject({
      status: 415,
      errorClass: "client-fault",
      body: {
        error: {
          type: "invalid_request_error",
          code: "unsupported_content_type",
        },
      },
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

});
