import Fastify, { type FastifyInstance } from "fastify";
import sensible from "@fastify/sensible";
import { healthRoute } from "./routes/health.js";
import { authPreHandler } from "./middleware/auth.js";
import { createDb, type DrizzleClient } from "./db/client.js";
import { getPepper } from "./config.js";

interface BuildAppOptions {
  logger?: boolean | Record<string, unknown>;
  db?: DrizzleClient;
  registerProtected?: (scope: FastifyInstance) => Promise<void>;
}

export async function buildApp(
  options: BuildAppOptions = {},
): Promise<FastifyInstance> {
  getPepper();

  const app = Fastify({
    logger: options.logger ?? {
      level: process.env["LOG_LEVEL"] ?? "info",
      transport:
        process.env["NODE_ENV"] === "development"
          ? {
              target: "pino-pretty",
              options: { translateTime: "HH:mm:ss.l", ignore: "pid,hostname" },
            }
          : undefined,
    },
  });

  let db: DrizzleClient;
  let closeDb: (() => Promise<void>) | undefined;
  if (options.db) {
    db = options.db;
  } else {
    const databaseUrl = process.env["DATABASE_URL"];
    if (!databaseUrl) {
      throw new Error("DATABASE_URL environment variable is required");
    }
    const created = createDb(databaseUrl);
    db = created.db;
    closeDb = created.close;
  }

  app.decorate("db", db);
  app.decorateRequest("tenantId", null);
  app.decorateRequest("planTier", null);

  if (closeDb) {
    app.addHook("onClose", closeDb);
  }

  await app.register(sensible);
  await app.register(healthRoute);

  await app.register(async (scope) => {
    scope.addHook("onRequest", authPreHandler);
    if (options.registerProtected) {
      await options.registerProtected(scope);
    }
  });

  return app;
}
