import type { FastifyInstance } from "fastify";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import packageJson from "../package.json" with { type: "json" };
import { buildApp } from "../src/app.js";

interface HealthResponse {
  ok: boolean;
  version: string;
  uptime: number;
}

describe("GET /health", () => {
  let app: FastifyInstance;

  let savedDatabaseUrl: string | undefined;

  beforeAll(async () => {
    savedDatabaseUrl = process.env['DATABASE_URL'];
    process.env['GATEWAY_HMAC_PEPPER'] = 'test-pepper-must-be-at-least-32-characters-long';
    process.env['DATABASE_URL'] = 'postgresql://localhost:5432/llm_gateway_test';
    app = await buildApp({ logger: false });
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
    delete process.env['GATEWAY_HMAC_PEPPER'];
    if (savedDatabaseUrl !== undefined) {
      process.env['DATABASE_URL'] = savedDatabaseUrl;
    } else {
      delete process.env['DATABASE_URL'];
    }
  });

  it("returns 200", async () => {
    const res = await app.inject({ method: "GET", url: "/health" });

    expect(res.statusCode).toBe(200);
  });

  it("returns service health payload", async () => {
    const res = await app.inject({ method: "GET", url: "/health" });
    const body = JSON.parse(res.payload) as HealthResponse;

    expect(res.statusCode).toBe(200);
    expect(body.ok).toBe(true);
    expect(body.version).toBe(packageJson.version);
    expect(typeof body.uptime).toBe("number");
    expect(body.uptime).toBeGreaterThanOrEqual(0);
  });
});
