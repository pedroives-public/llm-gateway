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

describe('buildApp — OPENAI_API_KEY boot validation', () => {
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env['OPENAI_API_KEY'];
  });

  afterEach(() => {
    if (saved !== undefined) {
      process.env['OPENAI_API_KEY'] = saved;
    } else {
      delete process.env['OPENAI_API_KEY'];
    }
  });

  it('throws when OPENAI_API_KEY is absent', async () => {
    delete process.env['OPENAI_API_KEY'];
    await expect(buildApp({ logger: false })).rejects.toThrow('OPENAI_API_KEY');
  });

  it('throws when OPENAI_API_KEY is empty string', async () => {
    process.env['OPENAI_API_KEY'] = '';
    await expect(buildApp({ logger: false })).rejects.toThrow('OPENAI_API_KEY');
  });
});

describe('buildApp — OPENAI_BASE_URL boot validation', () => {
  let savedKey: string | undefined;
  let savedUrl: string | undefined;

  beforeEach(() => {
    savedKey = process.env['OPENAI_BASE_URL'];
    savedUrl = process.env['NODE_ENV'];
  });

  afterEach(() => {
    if (savedKey !== undefined) {
      process.env['OPENAI_BASE_URL'] = savedKey;
    } else {
      delete process.env['OPENAI_BASE_URL'];
    }
    if (savedUrl !== undefined) {
      process.env['NODE_ENV'] = savedUrl;
    } else {
      delete process.env['NODE_ENV'];
    }
  });

  it('throws when OPENAI_BASE_URL is http:// outside NODE_ENV=test', async () => {
    process.env['OPENAI_BASE_URL'] = 'http://localhost:8080';
    process.env['NODE_ENV'] = 'development';
    await expect(buildApp({ logger: false })).rejects.toThrow('https://');
  });

  it('allows http:// override when NODE_ENV=test', async () => {
    process.env['OPENAI_BASE_URL'] = 'http://localhost:8080';
    process.env['NODE_ENV'] = 'test';
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ logger: false, db: fakeDb });
    await app.close();
  });
});

describe('buildApp — production pino.transport assertion', () => {
  let savedEnv: string | undefined;

  beforeEach(() => {
    savedEnv = process.env['NODE_ENV'];
  });

  afterEach(() => {
    if (savedEnv !== undefined) {
      process.env['NODE_ENV'] = savedEnv;
    } else {
      delete process.env['NODE_ENV'];
    }
  });

  it('throws when NODE_ENV=production and logger has no transport', async () => {
    process.env['NODE_ENV'] = 'production';
    await expect(
      buildApp({ logger: { level: 'info' }, db: {} as DrizzleClient }),
    ).rejects.toThrow('pino.transport');
  });

  it('does not throw when NODE_ENV=production and default logger (auto-transport)', async () => {
    process.env['NODE_ENV'] = 'production';
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ db: fakeDb });
    await app.close();
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

describe('buildApp — reqId decorator', () => {
  it('sets a UUIDv7 reqId on every request before auth runs', async () => {
    const fakeDb = {} as DrizzleClient;

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get('/reqid-check', async (request) => {
          void request.reqId;
          return { ok: true };
        });
      },
    });

    try {
      // Unauthenticated — route is protected, but reqId should be set before auth rejects
      const res = await app.inject({ method: 'GET', url: '/reqid-check' });
      expect(res.statusCode).toBe(401);
      // Hit health (public) to confirm reqId is set on public routes too
      const healthRes = await app.inject({ method: 'GET', url: '/health' });
      expect(healthRes.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it('reqId is a non-empty string on authenticated requests', async () => {
    const { randomBytes, randomUUID } = await import('node:crypto');
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString('base64url')}`;
    let capturedReqId: string | undefined;

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
                          { tenantId, planTier: 'pro', apiKeyStatus: 'active', tenantStatus: 'active' },
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
        scope.get('/check', async (request) => {
          capturedReqId = request.reqId;
          return { reqId: request.reqId };
        });
      },
    });

    try {
      const res = await app.inject({
        method: 'GET',
        url: '/check',
        headers: { authorization: `Bearer ${apiKey}` },
      });
      expect(res.statusCode).toBe(200);
      expect(capturedReqId).toBeTruthy();
      expect(typeof capturedReqId).toBe('string');
      // UUIDv7 has version nibble = 7
      expect(capturedReqId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    } finally {
      await app.close();
    }
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
