import { randomUUID } from "node:crypto";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute, type ProxyRouteOptions } from "../../src/routes/proxy.js";
import type { DrizzleClient } from "../../src/db/client.js";
import { fakeAuthDb } from "./fake-auth-db.js";

// buildApp with only the proxy route registered — the wiring every proxy test
// repeats. db defaults to the happy-path auth fake; pass a real client to
// exercise auth itself. logger defaults to off; pass a log capture's logger to
// observe events. The streaming flag is a parameter here, OFF unless a
// test asks for ON; tests never switch it through process.env.
export async function buildProxyApp(
  opts: ProxyRouteOptions & {
    db?: DrizzleClient;
    logger?: boolean | Record<string, unknown>;
  },
): Promise<FastifyInstance> {
  return buildApp({
    logger: opts.logger ?? false,
    db: opts.db ?? fakeAuthDb(randomUUID()),
    registerProtected: async (scope) => {
      await scope.register(proxyRoute, {
        breaker: opts.breaker,
        streamingEnabled: opts.streamingEnabled ?? false,
        upstreamBuffered: opts.upstreamBuffered,
        upstreamStreaming: opts.upstreamStreaming,
      });
    },
  });
}
