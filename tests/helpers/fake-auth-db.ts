import { randomBytes } from "node:crypto";
import type { DrizzleClient } from "../../src/db/client.js";

// One valid active row for any presented key, so an authenticated request
// reaches the route under test. Auth edge cases (bad hash, unknown key,
// revoked rows) live in tests/middleware/auth.test.ts, which builds its own
// fakes against a real Postgres row set.
export function fakeAuthDb(tenantId: string): DrizzleClient {
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

// Authorization header carrying a well-formed throwaway key. Tests that need
// the raw key (to control its HMAC digest) build their own instead.
export const bearer = (): string =>
  `Bearer lkey_${randomBytes(32).toString("base64url")}`;
