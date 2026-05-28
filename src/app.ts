import Fastify, { type FastifyInstance } from "fastify";
import sensible from "@fastify/sensible";
import { v7 as uuidv7 } from "uuid";
import { healthRoute } from "./routes/health.js";
import { authPreHandler } from "./middleware/auth.js";
import { createDb, type DrizzleClient } from "./db/client.js";
import { getPepper, getOpenAIApiKey, getOpenAIBaseUrl } from "./config.js";

interface BuildAppOptions {
  logger?: boolean | Record<string, unknown>;
  db?: DrizzleClient;
  registerProtected?: (scope: FastifyInstance) => Promise<void>;
}

export async function buildApp(
  options: BuildAppOptions = {},
): Promise<FastifyInstance> {
  getPepper();
  getOpenAIApiKey();
  getOpenAIBaseUrl();

  const nodeEnv = process.env["NODE_ENV"];

  const resolvedLogger = options.logger ?? {
    level: process.env["LOG_LEVEL"] ?? "info",
    transport:
      nodeEnv === "development"
        ? { target: "pino-pretty", options: { translateTime: "HH:mm:ss.l", ignore: "pid,hostname" } }
        : nodeEnv === "production"
          ? { target: "pino/file", options: { destination: 1 } }
          : undefined,
  };

  if (
    nodeEnv === "production" &&
    resolvedLogger !== false &&
    typeof resolvedLogger === "object" &&
    !("transport" in resolvedLogger && resolvedLogger.transport)
  ) {
    throw new Error(
      "pino.transport must be configured in NODE_ENV=production to avoid blocking the event loop",
    );
  }

  const app = Fastify({ logger: resolvedLogger });

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
  app.decorateRequest("reqId", "");
  app.addHook("onRequest", async (request) => {
    request.reqId = uuidv7();
  });

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
