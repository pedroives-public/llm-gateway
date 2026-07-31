import { createHmac } from "node:crypto";
import { eq } from "drizzle-orm";
import { apiKeys, planTierValues, tenants } from "../db/schema.js";
import type { PlanTier } from "../db/schema.js";
import { getPepper } from "../config.js";
import type { FastifyReply, FastifyRequest } from "fastify";

const PEPPER = getPepper();

const KEY_TRACE_LENGTH = 8;
const API_KEY_LENGTH = 48; // lkey_ (5) + 43 base64url chars (32 random bytes)

type AuthDbCause = { cause_code: string; cause_name: string };

// The postgres driver attaches diagnostic detail this log must not carry:
// connection errors expose the database address and port, PostgresError copies
// the server's protocol fields (relation names, source file), and every stack
// embeds absolute paths and the pinned driver version. Only the two
// identifying fields cross into the log.
function authDbCause(error: unknown): AuthDbCause {
  if (typeof error !== "object" || error === null) {
    return { cause_code: "UNKNOWN", cause_name: "UNKNOWN" };
  }
  return {
    cause_code:
      "code" in error && typeof error.code === "string"
        ? error.code
        : "UNKNOWN",
    cause_name:
      "name" in error && typeof error.name === "string"
        ? error.name
        : "UNKNOWN",
  };
}

// Domain-separated from the verification HMAC so the log never carries bits of
// the stored verifier; the prefix cannot collide with a verification message
// because every accepted key starts with lkey_. Truncation trades uniqueness
// against log noise, never secrecy — a digest is not reversible.
function keyTrace(apiKey: string): string {
  return createHmac("sha256", PEPPER)
    .update(`log-trace:${apiKey}`)
    .digest("hex")
    .slice(0, KEY_TRACE_LENGTH);
}

function isPlanTier(value: string): value is PlanTier {
  return planTierValues.includes(value as PlanTier);
}

function unauthorized(reply: FastifyReply): void {
  reply.code(401).send({ error: "Unauthorized" });
}

function serviceUnavailable(reply: FastifyReply): void {
  reply.code(503).send({ error: "Service Unavailable" });
}

function internalError(reply: FastifyReply): void {
  reply.code(500).send({ error: "Internal Server Error" });
}

export async function authPreHandler(
  request: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  const authorization = request.headers.authorization;

  if (
    typeof authorization !== "string" ||
    !authorization.startsWith("Bearer lkey_")
  ) {
    request.log.warn(
      { reason: "invalid_authorization_header" },
      "Auth rejected",
    );
    unauthorized(reply);
    return;
  }

  const apiKey = authorization.slice("Bearer ".length);

  if (apiKey.length !== API_KEY_LENGTH) {
    request.log.warn(
      { reason: "invalid_authorization_header" },
      "Auth rejected",
    );
    unauthorized(reply);
    return;
  }
  const keyTraceValue = keyTrace(apiKey);
  const apiKeyHashValue = createHmac("sha256", PEPPER)
    .update(apiKey)
    .digest("hex");

  try {
    const [findKey] = await request.server.db
      .select({
        tenantId: tenants.id,
        planTier: tenants.planTier,
        apiKeyStatus: apiKeys.status,
        tenantStatus: tenants.status,
      })
      .from(apiKeys)
      .innerJoin(tenants, eq(tenants.id, apiKeys.tenantId))
      .where(eq(apiKeys.hashValue, apiKeyHashValue))
      .limit(1);

    if (!findKey || findKey.apiKeyStatus !== "active") {
      request.log.warn(
        { keyTrace: keyTraceValue, reason: "unauthorized" },
        "Auth rejected",
      );
      unauthorized(reply);
      return;
    }

    if (findKey.tenantStatus !== "active") {
      request.log.warn(
        {
          keyTrace: keyTraceValue,
          reason: "tenant_suspended",
          tenantId: findKey.tenantId,
        },
        "Auth rejected",
      );
      unauthorized(reply);
      return;
    }

    if (!isPlanTier(findKey.planTier)) {
      request.log.error(
        {
          keyTrace: keyTraceValue,
          reason: "invalid_plan_tier",
          tenantId: findKey.tenantId,
        },
        "Auth: unrecognized plan tier in DB",
      );
      internalError(reply);
      return;
    }

    request.tenantId = findKey.tenantId;
    request.planTier = findKey.planTier;
    request.log.info(
      {
        keyTrace: keyTraceValue,
        tenantId: findKey.tenantId,
        planTier: findKey.planTier,
      },
      "Auth succeeded",
    );
  } catch (error) {
    request.log.error(authDbCause(error), "Auth DB lookup failed");
    serviceUnavailable(reply);
    return;
  }
}
