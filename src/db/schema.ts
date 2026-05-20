import { sql } from "drizzle-orm";
import {
  check,
  pgTable,
  uniqueIndex,
  uuid,
  varchar,
  timestamp,
} from "drizzle-orm/pg-core";

export const tenantStatusValues = ["active", "suspended"] as const;
export type TenantStatus = (typeof tenantStatusValues)[number];

export const planTierValues = ["free", "pro", "enterprise"] as const;
export type PlanTier = (typeof planTierValues)[number];

export const apiKeyStatusValues = ["active", "revoked"] as const;
export type ApiKeyStatus = (typeof apiKeyStatusValues)[number];

export const tenants = pgTable(
  "tenants",
  {
    id: uuid("id").primaryKey(),
    name: varchar("name", { length: 255 }).notNull(),
    status: varchar("status", { length: 20 }).notNull(),
    planTier: varchar("plan_tier", { length: 32 }).notNull(),
    createdAt: timestamp("created_at", {
      withTimezone: true,
      mode: "date",
    })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", {
      withTimezone: true,
      mode: "date",
    })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    check(
      "tenant_status_check",
      sql`${table.status} in (${sql.raw(tenantStatusValues.map((s) => `'${s}'`).join(", "))})`,
    ),
    check(
      "tenant_plan_tier_check",
      sql`${table.planTier} in (${sql.raw(planTierValues.map((s) => `'${s}'`).join(", "))})`,
    ),
  ],
);

export const apiKeys = pgTable(
  "api_keys",
  {
    id: uuid("id").primaryKey(),
    tenantId: uuid("tenant_id")
      .notNull()
      .references(() => tenants.id, { onDelete: "no action" }),
    hashValue: varchar("hash_value", { length: 64 }).notNull(),
    status: varchar("status", { length: 20 }).notNull(),
    createdAt: timestamp("created_at", {
      withTimezone: true,
      mode: "date",
    })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    uniqueIndex("api_keys_hash_value_unique").on(table.hashValue),
    check(
      "api_key_status_check",
      sql`${table.status} in (${sql.raw(apiKeyStatusValues.map((s) => `'${s}'`).join(", "))})`,
    ),
  ],
);
