import { createHmac, randomBytes, randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { fileURLToPath } from "node:url";
import path from "node:path";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import type { CircuitBreaker } from "../../src/reliability/circuit-breaker.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { createDb, type DrizzleClient } from "../../src/db/client.js";
import { apiKeys, tenants } from "../../src/db/schema.js";
import { TEST_PEPPER } from "../constants.js";

// Composition tests, one seam each: the size cap through the real client and
// breaker (auth faked), and real pepper+HMAC auth through the route (upstream
// stubbed). Everything else in the suite covers these seams in halves.

const validBody = {
  model: "gpt-4o",
  messages: [{ role: "user", content: "hi" }],
};

// Same shape as proxy.test.ts's fakeAuthDb: one valid active row so an
// authenticated request reaches the route regardless of the presented key.
function fakeAuthDb(tenantId: string): DrizzleClient {
  return {
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
                          tenantId,
                          planTier: "pro",
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
}

describe("8.23 — >1 MiB upstream through the REAL client + route + breaker", () => {
  it(
    "responds 502 response_too_large on every attempt — six capped reads never open the breaker",
    { timeout: 30_000 },
    async () => {
      const CHUNK = 64 * 1024;
      const chunk = Buffer.alloc(CHUNK, 0x61);
      const TARGET = 8 * 1024 * 1024; // far above the 1 MiB cap + buffer window
      const perRequestBytes: number[] = [];

      const server = http.createServer((_req, res) => {
        res.on("error", () => {});
        res.writeHead(200, { "content-type": "application/json" });
        const idx = perRequestBytes.push(0) - 1;
        let sent = 0;
        const pump = (): void => {
          if (res.destroyed || res.writableEnded) return;
          while (sent < TARGET) {
            const ok = res.write(chunk);
            sent += CHUNK;
            perRequestBytes[idx] = sent;
            if (!ok) {
              res.once("drain", pump);
              return;
            }
          }
          res.end();
        };
        pump();
      });
      await new Promise<void>((resolve) =>
        server.listen(0, "127.0.0.1", resolve),
      );
      const { port } = server.address() as AddressInfo;

      const prevBaseUrl = process.env["OPENAI_BASE_URL"];
      process.env["OPENAI_BASE_URL"] = `http://127.0.0.1:${port}`;

      // No registerProtected → buildApp wires the real breaker + real client.
      const app = await buildApp({
        logger: false,
        db: fakeAuthDb(randomUUID()),
      });
      const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;

      try {
        // Six attempts: if a capped read incremented the breaker, the fifth
        // would open it and the sixth would fast-fail 503 instead of 502.
        for (let i = 1; i <= 6; i++) {
          const res = await app.inject({
            method: "POST",
            url: "/v1/chat/completions",
            headers: { authorization: `Bearer ${apiKey}` },
            payload: validBody,
          });
          expect(res.statusCode, `request ${i}`).toBe(502);
          expect(res.headers["x-gateway-error-class"]).toBe("upstream-fault");
          expect(JSON.parse(res.payload)).toEqual({
            error: {
              message: "upstream response too large",
              type: "server_error",
              code: "response_too_large",
            },
          });
        }
        // Read-cancel reached the producer: no request drained the full body.
        await new Promise((resolve) => setTimeout(resolve, 300));
        expect(perRequestBytes.length).toBe(6);
        for (const sent of perRequestBytes) {
          expect(sent).toBeLessThan(TARGET);
        }
      } finally {
        await app.close();
        await new Promise<void>((resolve) => server.close(() => resolve()));
        if (prevBaseUrl === undefined) {
          delete process.env["OPENAI_BASE_URL"];
        } else {
          process.env["OPENAI_BASE_URL"] = prevBaseUrl;
        }
      }
    },
  );
});

describe("8.2 — real pepper + HMAC + Postgres row through the proxy route", () => {
  const MIGRATIONS_FOLDER = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../drizzle",
  );

  function hmacDigest(key: string): string {
    return createHmac("sha256", TEST_PEPPER).update(key).digest("hex");
  }

  let container: StartedPostgreSqlContainer;
  let db: DrizzleClient;
  let closeDb: () => Promise<void>;
  let app: FastifyInstance;
  let prevPepper: string | undefined;
  let upstreamCalls = 0;

  beforeAll(async () => {
    prevPepper = process.env["GATEWAY_HMAC_PEPPER"];
    process.env["GATEWAY_HMAC_PEPPER"] = TEST_PEPPER;

    container = await new PostgreSqlContainer("postgres:17-alpine").start();
    const migrationSql = postgres(container.getConnectionUri(), { max: 1 });
    await migrate(drizzle(migrationSql), {
      migrationsFolder: MIGRATIONS_FOLDER,
    });
    await migrationSql.end();

    const created = createDb(container.getConnectionUri());
    db = created.db;
    closeDb = created.close;

    const stubBreaker: CircuitBreaker = {
      tryAcquire: () => ({ kind: "NORMAL" }),
      recordResult: () => {},
      getState: () => "CLOSED",
    };
    const countingUpstream = (): Promise<Outcome> => {
      upstreamCalls += 1;
      return Promise.resolve({ kind: "ok", status: 200, body_parsed: {} });
    };

    app = await buildApp({
      logger: false,
      db,
      registerProtected: async (scope) => {
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: countingUpstream,
        });
      },
    });
  }, 120_000);

  afterAll(async () => {
    await app?.close();
    await closeDb?.();
    await container?.stop();
    if (prevPepper === undefined) {
      delete process.env["GATEWAY_HMAC_PEPPER"];
    } else {
      process.env["GATEWAY_HMAC_PEPPER"] = prevPepper;
    }
  });

  it("a key whose HMAC(pepper) row is active reaches the proxy route (200)", async () => {
    const tenantId = randomUUID();
    await db
      .insert(tenants)
      .values({ id: tenantId, name: "T", status: "active", planTier: "pro" });
    const key = `lkey_${randomBytes(32).toString("base64url")}`;
    await db.insert(apiKeys).values({
      id: randomUUID(),
      tenantId,
      hashValue: hmacDigest(key),
      status: "active",
    });

    const res = await app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: `Bearer ${key}` },
      payload: validBody,
    });

    expect(res.statusCode).toBe(200);
    expect(upstreamCalls).toBe(1);
  });

  it("a fresh key with no matching hash is rejected 401 before the route", async () => {
    const callsBefore = upstreamCalls;
    const res = await app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: {
        authorization: `Bearer lkey_${randomBytes(32).toString("base64url")}`,
      },
      payload: validBody,
    });

    expect(res.statusCode).toBe(401);
    expect(upstreamCalls).toBe(callsBefore); // never reached the route
  });
});
