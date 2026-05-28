declare module "fastify" {
  interface FastifyInstance {
    db: import("../db/client.ts").DrizzleClient;
  }
  interface FastifyRequest {
    tenantId: string | null;
    planTier: import("../db/schema.ts").PlanTier | null;
    reqId: string;
  }
}

export {};
