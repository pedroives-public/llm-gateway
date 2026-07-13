import { randomUUID } from "node:crypto";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute, type ProxyRouteOptions } from "../../src/routes/proxy.js";
import type { DrizzleClient } from "../../src/db/client.js";
import { fakeAuthDb } from "./fake-auth-db.js";

// buildApp with only the proxy route registered — the wiring every proxy test
// repeats. db defaults to the happy-path auth fake; pass a real client to
// exercise auth itself.
export async function buildProxyApp(
  opts: ProxyRouteOptions & { db?: DrizzleClient },
): Promise<FastifyInstance> {
  return buildApp({
    logger: false,
    db: opts.db ?? fakeAuthDb(randomUUID()),
    registerProtected: async (scope) => {
      await scope.register(proxyRoute, {
        breaker: opts.breaker,
        upstreamBuffered: opts.upstreamBuffered,
      });
    },
  });
}
