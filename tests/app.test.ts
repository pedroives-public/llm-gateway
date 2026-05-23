import { randomBytes, randomUUID } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { buildApp } from '../src/app.js';
import type { DrizzleClient } from '../src/db/client.js';

describe('buildApp — pepper boot validation', () => {
  let savedPepper: string | undefined;

  beforeEach(() => {
    savedPepper = process.env['GATEWAY_HMAC_PEPPER'];
  });

  afterEach(() => {
    if (savedPepper !== undefined) {
      process.env['GATEWAY_HMAC_PEPPER'] = savedPepper;
    } else {
      delete process.env['GATEWAY_HMAC_PEPPER'];
    }
  });

  it('throws when GATEWAY_HMAC_PEPPER is absent', async () => {
    delete process.env['GATEWAY_HMAC_PEPPER'];
    await expect(buildApp({ logger: false })).rejects.toThrow('GATEWAY_HMAC_PEPPER');
  });

  it('throws when GATEWAY_HMAC_PEPPER is empty string', async () => {
    process.env['GATEWAY_HMAC_PEPPER'] = '';
    await expect(buildApp({ logger: false })).rejects.toThrow('GATEWAY_HMAC_PEPPER');
  });

  it('throws when GATEWAY_HMAC_PEPPER is shorter than 32 characters', async () => {
    process.env['GATEWAY_HMAC_PEPPER'] = 'a'.repeat(31);
    await expect(buildApp({ logger: false })).rejects.toThrow('32 characters');
  });

  it('throws when GATEWAY_HMAC_PEPPER is the known default value', async () => {
    process.env['GATEWAY_HMAC_PEPPER'] = 'change-me-min-32-chars-recommended';
    await expect(buildApp({ logger: false })).rejects.toThrow('default');
  });
});

describe('buildApp — DATABASE_URL boot validation', () => {
  let savedDatabaseUrl: string | undefined;

  beforeEach(() => {
    savedDatabaseUrl = process.env['DATABASE_URL'];
  });

  afterEach(() => {
    if (savedDatabaseUrl !== undefined) {
      process.env['DATABASE_URL'] = savedDatabaseUrl;
    } else {
      delete process.env['DATABASE_URL'];
    }
  });

  it('throws when DATABASE_URL is absent and no db option provided', async () => {
    delete process.env['DATABASE_URL'];
    await expect(buildApp({ logger: false })).rejects.toThrow('DATABASE_URL');
  });
});

describe('buildApp — protected scope integration', () => {
  it('enforces auth on routes registered inside the protected scope', async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString('base64url')}`;

    // The fake db always resolves to a valid row; the test verifies the wiring
    // (request reaches the route, decorations populated). Hash mismatch and
    // unknown-key paths are covered by tests/middleware/auth.test.ts.
    const fakeDb = {
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
                            planTier: 'pro',
                            apiKeyStatus: 'active',
                            tenantStatus: 'active',
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

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get('/ping', async (request) => ({
          tenantId: request.tenantId,
          planTier: request.planTier,
        }));
      },
    });

    try {
      const noAuth = await app.inject({ method: 'GET', url: '/ping' });
      expect(noAuth.statusCode).toBe(401);

      const withAuth = await app.inject({
        method: 'GET',
        url: '/ping',
        headers: { authorization: `Bearer ${apiKey}` },
      });
      expect(withAuth.statusCode).toBe(200);
      expect(JSON.parse(withAuth.payload)).toEqual({ tenantId, planTier: 'pro' });
    } finally {
      await app.close();
    }
  });

  it('leaves health route public (registered outside the protected scope)', async () => {
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ logger: false, db: fakeDb });

    try {
      const res = await app.inject({ method: 'GET', url: '/health' });
      expect(res.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });
});
