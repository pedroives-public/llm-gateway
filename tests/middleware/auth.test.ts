import { createHmac, randomBytes, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import { sql } from "drizzle-orm";
import Fastify, { type FastifyInstance } from "fastify";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { authPreHandler } from "../../src/middleware/auth.js";
import { createDb, type DrizzleClient } from "../../src/db/client.js";
import { apiKeys, tenants } from "../../src/db/schema.js";
import { TEST_PEPPER } from "../constants.js";

const MIGRATIONS_FOLDER = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../drizzle",
);

function hmacDigest(key: string): string {
  return createHmac("sha256", TEST_PEPPER).update(key).digest("hex");
}

function freshKey(): string {
  return `lkey_${randomBytes(32).toString("base64url")}`;
}

let container: StartedPostgreSqlContainer;
let db: DrizzleClient;
let closeDb: () => Promise<void>;
let app: FastifyInstance;

beforeAll(async () => {
  process.env["GATEWAY_HMAC_PEPPER"] = TEST_PEPPER;

  container = await new PostgreSqlContainer("postgres:17-alpine").start();

  const migrationSql = postgres(container.getConnectionUri(), { max: 1 });
  await migrate(drizzle(migrationSql), { migrationsFolder: MIGRATIONS_FOLDER });
  await migrationSql.end();

  const created = createDb(container.getConnectionUri());
  db = created.db;
  closeDb = created.close;

  app = Fastify({ logger: false });
  app.decorate("db", db);
  app.decorateRequest("tenantId", null);
  app.decorateRequest("planTier", null);

  await app.register(async (scope) => {
    scope.addHook("onRequest", authPreHandler);
    scope.get("/protected", async (request) => ({
      tenantId: request.tenantId,
      planTier: request.planTier,
    }));
  });

  await app.ready();
}, 60_000);

afterAll(async () => {
  await app.close();
  await closeDb();
  await container.stop();
  delete process.env["GATEWAY_HMAC_PEPPER"];
});

beforeEach(async () => {
  await db.execute(sql`TRUNCATE TABLE api_keys, tenants CASCADE`);
});

describe("auth middleware — format validation (no DB)", () => {
  it("4.1 rejects request with no Authorization header", async () => {
    const res = await app.inject({ method: "GET", url: "/protected" });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.2 rejects non-Bearer Authorization scheme", async () => {
    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: "Basic dXNlcjpwYXNz" },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.3 rejects Bearer token without lkey_ prefix", async () => {
    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: "Bearer no-lkey-prefix-here" },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.3b rejects lkey_ token one char too short (47 chars)", async () => {
    const shortKey = `lkey_${"a".repeat(42)}`; // 5 + 42 = 47
    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${shortKey}` },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.3c rejects lkey_ token one char too long (49 chars)", async () => {
    const longKey = `lkey_${"a".repeat(44)}`; // 5 + 44 = 49
    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${longKey}` },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });
});

describe("auth middleware — DB-backed verification", () => {
  it("4.4 rejects a valid-format key with no matching hash in the DB", async () => {
    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${freshKey()}` },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.5 rejects a key whose api_keys row is revoked", async () => {
    const tenantId = randomUUID();
    await db
      .insert(tenants)
      .values({ id: tenantId, name: "T", status: "active", planTier: "free" });
    const key = freshKey();
    await db.insert(apiKeys).values({
      id: randomUUID(),
      tenantId,
      hashValue: hmacDigest(key),
      status: "revoked",
    });

    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${key}` },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.6 returns 401 when the tenant is suspended", async () => {
    const tenantId = randomUUID();
    await db.insert(tenants).values({
      id: tenantId,
      name: "T",
      status: "suspended",
      planTier: "pro",
    });
    const key = freshKey();
    await db.insert(apiKeys).values({
      id: randomUUID(),
      tenantId,
      hashValue: hmacDigest(key),
      status: "active",
    });

    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${key}` },
    });

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.payload)).toEqual({ error: "Unauthorized" });
  });

  it("4.7 returns 200 and attaches tenantId + planTier on valid key and active tenant", async () => {
    const tenantId = randomUUID();
    await db.insert(tenants).values({
      id: tenantId,
      name: "T",
      status: "active",
      planTier: "enterprise",
    });
    const key = freshKey();
    await db.insert(apiKeys).values({
      id: randomUUID(),
      tenantId,
      hashValue: hmacDigest(key),
      status: "active",
    });

    const res = await app.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${key}` },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload) as {
      tenantId: string;
      planTier: string;
    };
    expect(body.tenantId).toBe(tenantId);
    expect(body.planTier).toBe("enterprise");
  });

  it("4.8 returns 503 when the DB throws synchronously", async () => {
    const errorDb = {
      select() {
        throw new Error("connection refused");
      },
    } as unknown as DrizzleClient;

    const isolatedApp = Fastify({ logger: false });
    isolatedApp.decorate("db", errorDb);
    isolatedApp.decorateRequest("tenantId", null);
    isolatedApp.decorateRequest("planTier", null);

    await isolatedApp.register(async (scope) => {
      scope.addHook("onRequest", authPreHandler);
      scope.get("/protected", async () => ({ ok: true }));
    });

    await isolatedApp.ready();

    const key = freshKey();
    const res = await isolatedApp.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${key}` },
    });

    await isolatedApp.close();

    expect(res.statusCode).toBe(503);
    expect(JSON.parse(res.payload)).toEqual({ error: "Service Unavailable" });
  });

  it("4.8b returns 503 when the DB query rejects asynchronously", async () => {
    const asyncErrorDb = {
      select() {
        return {
          from() {
            return {
              innerJoin() {
                return {
                  where() {
                    return {
                      limit() {
                        return Promise.reject(new Error("statement timeout"));
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

    const isolatedApp = Fastify({ logger: false });
    isolatedApp.decorate("db", asyncErrorDb);
    isolatedApp.decorateRequest("tenantId", null);
    isolatedApp.decorateRequest("planTier", null);

    await isolatedApp.register(async (scope) => {
      scope.addHook("onRequest", authPreHandler);
      scope.get("/protected", async () => ({ ok: true }));
    });

    await isolatedApp.ready();

    const key = freshKey();
    const res = await isolatedApp.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${key}` },
    });

    await isolatedApp.close();

    expect(res.statusCode).toBe(503);
    expect(JSON.parse(res.payload)).toEqual({ error: "Service Unavailable" });
  });

  it("4.10 returns 500 when DB returns an unrecognised plan_tier value", async () => {
    const bogusDb = {
      select() {
        return {
          from() {
            return {
              innerJoin() {
                return {
                  where() {
                    return {
                      limit() {
                        return Promise.resolve([
                          {
                            tenantId: randomUUID(),
                            planTier: "legacy",
                            apiKeyStatus: "active",
                            tenantStatus: "active",
                          },
                        ]);
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

    const isolatedApp = Fastify({ logger: false });
    isolatedApp.decorate("db", bogusDb);
    isolatedApp.decorateRequest("tenantId", null);
    isolatedApp.decorateRequest("planTier", null);

    await isolatedApp.register(async (scope) => {
      scope.addHook("onRequest", authPreHandler);
      scope.get("/protected", async () => ({ ok: true }));
    });

    await isolatedApp.ready();

    const key = freshKey();
    const res = await isolatedApp.inject({
      method: "GET",
      url: "/protected",
      headers: { authorization: `Bearer ${key}` },
    });

    await isolatedApp.close();

    expect(res.statusCode).toBe(500);
    expect(JSON.parse(res.payload)).toEqual({ error: "Internal Server Error" });
  });
});

describe("GET /health — auth middleware does not apply", () => {
  it("4.9 returns 200 without Authorization header", async () => {
    const { healthRoute } = await import("../../src/routes/health.js");

    const healthApp = Fastify({ logger: false });
    healthApp.decorate("db", db);
    healthApp.decorateRequest("tenantId", null);
    healthApp.decorateRequest("planTier", null);
    await healthApp.register(healthRoute);
    await healthApp.ready();

    const res = await healthApp.inject({ method: "GET", url: "/health" });

    await healthApp.close();

    expect(res.statusCode).toBe(200);
  });
});
