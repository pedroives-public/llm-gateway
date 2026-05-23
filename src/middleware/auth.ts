import { createHmac } from "node:crypto";
import { eq } from "drizzle-orm";
import { apiKeys, planTierValues, tenants } from "../db/schema.js";
import type { PlanTier } from "../db/schema.js";
import { getPepper } from "../config.js";
import type { FastifyReply, FastifyRequest } from "fastify";

const PEPPER = getPepper();

const TRACE_PREFIX_LENGTH = 13;
const API_KEY_LENGTH = 48; // lkey_ (5) + 43 base64url chars (32 random bytes)

function keyPrefix(apiKey: string): string {
  return `${apiKey.slice(0, TRACE_PREFIX_LENGTH)}...`;
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
    request.log.warn({ reason: "invalid_authorization_header" }, "Auth rejected");
    unauthorized(reply);
    return;
  }

  const apiKey = authorization.slice("Bearer ".length);

  if (apiKey.length !== API_KEY_LENGTH) {
    request.log.warn({ reason: "invalid_authorization_header" }, "Auth rejected");
    unauthorized(reply);
    return;
  }
  const keyPrefixValue = keyPrefix(apiKey);
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
      request.log.warn({ keyPrefix: keyPrefixValue, reason: "unauthorized" }, "Auth rejected");
      unauthorized(reply);
      return;
    }

    if (findKey.tenantStatus !== "active") {
      request.log.warn(
        {
          keyPrefix: keyPrefixValue,
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
          keyPrefix: keyPrefixValue,
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
        keyPrefix: keyPrefixValue,
        tenantId: findKey.tenantId,
        planTier: findKey.planTier,
      },
      "Auth succeeded",
    );
  } catch (error) {
    request.log.error({ err: error, keyPrefix: keyPrefixValue }, "Auth DB lookup failed");
    serviceUnavailable(reply);
    return;
  }
}
