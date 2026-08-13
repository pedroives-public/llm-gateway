import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import * as schema from "./schema.js";

// K_pre derivation inputs (see ADMISSION_CAPACITY_PRE_AUTH in config.ts),
// pinned by tests/reliability/admission-derivation-pin.test.ts: changing
// either re-opens that derivation. max is explicit so the input lives in
// this repo, not in a library default a version bump can move.
export const DEFAULT_DB_STATEMENT_TIMEOUT_MS = 2000;
export const DB_POOL_MAX = 10;

function resolveDbStatementTimeoutMs(): number {
  const rawTimeout = process.env["GATEWAY_DB_STATEMENT_TIMEOUT_MS"];

  if (rawTimeout === undefined || rawTimeout === "") {
    return DEFAULT_DB_STATEMENT_TIMEOUT_MS;
  }

  const timeoutMs = Number(rawTimeout);
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error(
      "GATEWAY_DB_STATEMENT_TIMEOUT_MS must be a positive integer",
    );
  }

  return timeoutMs;
}

export type DrizzleClient = PostgresJsDatabase<typeof schema>;

export interface CreatedDb {
  db: DrizzleClient;
  close: () => Promise<void>;
}

// statement_timeout is set at the connection (session) level via the postgres.js
// `connection` option, so it applies to every query on this pool — not just auth.
// When other query types (proxy, billing) need different bounds, give them a
// separate pool or wrap them in BEGIN; SET LOCAL statement_timeout; ... COMMIT.
// Never set debug: true in production — postgres.js logs query parameters,
// which would include HMAC hash values on auth queries.
export function createDb(connectionString: string): CreatedDb {
  const client = postgres(connectionString, {
    max: DB_POOL_MAX,
    connection: {
      statement_timeout: resolveDbStatementTimeoutMs(),
    },
  });
  return {
    db: drizzle(client, { schema }),
    close: () => client.end({ timeout: 5 }),
  };
}
