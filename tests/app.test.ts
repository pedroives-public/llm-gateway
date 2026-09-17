import { randomBytes, randomUUID } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { buildApp } from "../src/app.js";
import type { DrizzleClient } from "../src/db/client.js";
import { bearer, fakeAuthDb } from "./helpers/fake-auth-db.js";
import { HTTP_SERVER_OPTIONS } from "../src/config.js";

describe("buildApp — pepper boot validation", () => {
  let savedPepper: string | undefined;

  beforeEach(() => {
    savedPepper = process.env["GATEWAY_HMAC_PEPPER"];
  });

  afterEach(() => {
    if (savedPepper !== undefined) {
      process.env["GATEWAY_HMAC_PEPPER"] = savedPepper;
    } else {
      delete process.env["GATEWAY_HMAC_PEPPER"];
    }
  });

  it("throws when GATEWAY_HMAC_PEPPER is absent", async () => {
    delete process.env["GATEWAY_HMAC_PEPPER"];
    await expect(buildApp({ logger: false })).rejects.toThrow(
      "GATEWAY_HMAC_PEPPER",
    );
  });

  it("throws when GATEWAY_HMAC_PEPPER is empty string", async () => {
    process.env["GATEWAY_HMAC_PEPPER"] = "";
    await expect(buildApp({ logger: false })).rejects.toThrow(
      "GATEWAY_HMAC_PEPPER",
    );
  });

  it("throws when GATEWAY_HMAC_PEPPER is shorter than 32 characters", async () => {
    process.env["GATEWAY_HMAC_PEPPER"] = "a".repeat(31);
    await expect(buildApp({ logger: false })).rejects.toThrow("32 characters");
  });

  it("throws when GATEWAY_HMAC_PEPPER is the known default value", async () => {
    process.env["GATEWAY_HMAC_PEPPER"] = "change-me-min-32-chars-recommended";
    await expect(buildApp({ logger: false })).rejects.toThrow("default");
  });
});

describe("buildApp — OPENAI_API_KEY boot validation", () => {
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env["OPENAI_API_KEY"];
  });

  afterEach(() => {
    if (saved !== undefined) {
      process.env["OPENAI_API_KEY"] = saved;
    } else {
      delete process.env["OPENAI_API_KEY"];
    }
  });

  it("throws when OPENAI_API_KEY is absent", async () => {
    delete process.env["OPENAI_API_KEY"];
    await expect(buildApp({ logger: false })).rejects.toThrow("OPENAI_API_KEY");
  });

  it("throws when OPENAI_API_KEY is empty string", async () => {
    process.env["OPENAI_API_KEY"] = "";
    await expect(buildApp({ logger: false })).rejects.toThrow("OPENAI_API_KEY");
  });
});

describe("buildApp — OPENAI_BASE_URL boot validation", () => {
  let savedKey: string | undefined;
  let savedUrl: string | undefined;

  beforeEach(() => {
    savedKey = process.env["OPENAI_BASE_URL"];
    savedUrl = process.env["NODE_ENV"];
  });

  afterEach(() => {
    if (savedKey !== undefined) {
      process.env["OPENAI_BASE_URL"] = savedKey;
    } else {
      delete process.env["OPENAI_BASE_URL"];
    }
    if (savedUrl !== undefined) {
      process.env["NODE_ENV"] = savedUrl;
    } else {
      delete process.env["NODE_ENV"];
    }
  });

  it("throws when OPENAI_BASE_URL is http:// outside NODE_ENV=test", async () => {
    process.env["OPENAI_BASE_URL"] = "http://localhost:8080";
    process.env["NODE_ENV"] = "development";
    await expect(buildApp({ logger: false })).rejects.toThrow("https://");
  });

  it("allows http:// override when NODE_ENV=test", async () => {
    process.env["OPENAI_BASE_URL"] = "http://localhost:8080";
    process.env["NODE_ENV"] = "test";
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ logger: false, db: fakeDb });
    await app.close();
  });
});

describe("buildApp — STREAMING_ENABLED boot validation", () => {
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env["STREAMING_ENABLED"];
  });

  afterEach(() => {
    if (saved !== undefined) {
      process.env["STREAMING_ENABLED"] = saved;
    } else {
      delete process.env["STREAMING_ENABLED"];
    }
  });

  // Resolves to "booted" or to the boot error's message. The app is closed on
  // the boot path so a failing run never leaks a Fastify instance.
  async function bootOutcome(): Promise<string> {
    try {
      const app = await buildApp({ logger: false, db: {} as DrizzleClient });
      await app.close();
      return "booted";
    } catch (error) {
      return error instanceof Error ? error.message : String(error);
    }
  }

  it.each([
    ["TRUE"],
    ["False"],
    ["1"],
    ["0"],
    ["yes"],
    ["on"],
    ["off"],
    [" true"],
    ["true "],
    [""],
  ])("refuses to boot when STREAMING_ENABLED is %j", async (value) => {
    process.env["STREAMING_ENABLED"] = value;

    // The whole message is pinned, not a fragment: it names the variable and
    // the accepted spellings and carries nothing of the received value, so a
    // secret pasted into the wrong variable never reaches the boot log.
    expect(
      await bootOutcome(),
      "boot validation: a STREAMING_ENABLED value other than 'true' or 'false' must fail boot with the exact message, never be read as OFF and never echo the received value",
    ).toBe('STREAMING_ENABLED must be either "true" or "false"');
  });

  it.each([[undefined], ["true"], ["false"]])(
    "boots when STREAMING_ENABLED is %j",
    async (value) => {
      if (value === undefined) {
        delete process.env["STREAMING_ENABLED"];
      } else {
        process.env["STREAMING_ENABLED"] = value;
      }

      expect(
        await bootOutcome(),
        "boot validation: unset, 'true' and 'false' are the only accepted states and each must boot",
      ).toBe("booted");
    },
  );

  // Wiring pin: buildApp() must hand the accessor's value to the proxy route.
  // The body also carries `n: 2`, which the schema rejects AFTER `stream`, so
  // the request never passes validation and the real upstream client is never
  // reached, not even when a regression flips the flag. OFF reports the
  // `stream` violation first; ON has no `stream` violation left, so the first
  // one reported is `n`.
  it.each([
    { value: undefined, code: "stream_not_supported" },
    { value: "false", code: "stream_not_supported" },
    { value: "true", code: "n_not_supported" },
  ])(
    "STREAMING_ENABLED $value reaches the route: first rejection is $code",
    async ({ value, code }) => {
      if (value === undefined) {
        delete process.env["STREAMING_ENABLED"];
      } else {
        process.env["STREAMING_ENABLED"] = value;
      }

      const app = await buildApp({
        logger: false,
        db: fakeAuthDb(randomUUID()),
      });

      try {
        const res = await app.inject({
          method: "POST",
          url: "/v1/chat/completions",
          headers: { authorization: bearer() },
          payload: {
            model: "gpt-4o",
            messages: [{ role: "user", content: "hi" }],
            stream: true,
            n: 2,
          },
        });

        expect(
          { status: res.statusCode, code: res.json().error?.code },
          "streaming flag wiring: unset and 'false' keep rejecting stream:true at the schema; only 'true' lifts it, and the default when unset is OFF",
        ).toStrictEqual({ status: 400, code });
      } finally {
        await app.close();
      }
    },
  );

  describe("production guard", () => {
    let savedNodeEnv: string | undefined;

    beforeEach(() => {
      savedNodeEnv = process.env["NODE_ENV"];
    });

    afterEach(() => {
      if (savedNodeEnv !== undefined) {
        process.env["NODE_ENV"] = savedNodeEnv;
      } else {
        delete process.env["NODE_ENV"];
      }
    });

    // No streaming branch exists yet: with the flag ON a stream:true body
    // would reach the buffered read and count against the shared breaker. ON
    // is therefore refused in production, and only there. Remove this block
    // together with the guard in the change that turns streaming on.
    it.each([
      {
        nodeEnv: "production",
        value: "true",
        outcome:
          'STREAMING_ENABLED must not be "true" when NODE_ENV=production: the streaming path is not complete',
      },
      { nodeEnv: "production", value: "false", outcome: "booted" },
      { nodeEnv: "production", value: undefined, outcome: "booted" },
      {
        nodeEnv: "production",
        value: "TRUE",
        outcome: 'STREAMING_ENABLED must be either "true" or "false"',
      },
      { nodeEnv: "test", value: "true", outcome: "booted" },
      { nodeEnv: "development", value: "true", outcome: "booted" },
    ])(
      "NODE_ENV $nodeEnv with STREAMING_ENABLED $value -> $outcome",
      async ({ nodeEnv, value, outcome }) => {
        process.env["NODE_ENV"] = nodeEnv;
        if (value === undefined) {
          delete process.env["STREAMING_ENABLED"];
        } else {
          process.env["STREAMING_ENABLED"] = value;
        }

        expect(
          await bootOutcome(),
          "production guard: STREAMING_ENABLED=true must fail boot under NODE_ENV=production and nowhere else, and an invalid spelling still reports the spelling error first",
        ).toBe(outcome);
      },
    );
  });
});

describe("buildApp — production pino.transport assertion", () => {
  let savedEnv: string | undefined;

  beforeEach(() => {
    savedEnv = process.env["NODE_ENV"];
  });

  afterEach(() => {
    if (savedEnv !== undefined) {
      process.env["NODE_ENV"] = savedEnv;
    } else {
      delete process.env["NODE_ENV"];
    }
  });

  it("throws when NODE_ENV=production and logger has no transport", async () => {
    process.env["NODE_ENV"] = "production";
    await expect(
      buildApp({ logger: { level: "info" }, db: {} as DrizzleClient }),
    ).rejects.toThrow("pino.transport");
  });

  it("does not throw when NODE_ENV=production and default logger (auto-transport)", async () => {
    process.env["NODE_ENV"] = "production";
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ db: fakeDb });
    await app.close();
  });
});

describe("buildApp — DATABASE_URL boot validation", () => {
  let savedDatabaseUrl: string | undefined;

  beforeEach(() => {
    savedDatabaseUrl = process.env["DATABASE_URL"];
  });

  afterEach(() => {
    if (savedDatabaseUrl !== undefined) {
      process.env["DATABASE_URL"] = savedDatabaseUrl;
    } else {
      delete process.env["DATABASE_URL"];
    }
  });

  it("throws when DATABASE_URL is absent and no db option provided", async () => {
    delete process.env["DATABASE_URL"];
    await expect(buildApp({ logger: false })).rejects.toThrow("DATABASE_URL");
  });
});

describe("buildApp — reqId decorator", () => {
  it("sets a UUIDv7 reqId on every request before auth runs", async () => {
    const fakeDb = {} as DrizzleClient;

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get("/reqid-check", async (request) => {
          void request.reqId;
          return { ok: true };
        });
      },
    });

    try {
      // Unauthenticated — route is protected, but reqId should be set before auth rejects
      const res = await app.inject({ method: "GET", url: "/reqid-check" });
      expect(res.statusCode).toBe(401);
      // Hit health (public) to confirm reqId is set on public routes too
      const healthRes = await app.inject({ method: "GET", url: "/health" });
      expect(healthRes.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("reqId is a non-empty string on authenticated requests", async () => {
    const { randomBytes, randomUUID } = await import("node:crypto");
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    let capturedReqId: string | undefined;

    const fakeDb = fakeAuthDb(tenantId);

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get("/check", async (request) => {
          capturedReqId = request.reqId;
          return { reqId: request.reqId };
        });
      },
    });

    try {
      const res = await app.inject({
        method: "GET",
        url: "/check",
        headers: { authorization: `Bearer ${apiKey}` },
      });
      expect(res.statusCode).toBe(200);
      expect(capturedReqId).toBeTruthy();
      expect(typeof capturedReqId).toBe("string");
      // UUIDv7 has version nibble = 7
      expect(capturedReqId).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      );
    } finally {
      await app.close();
    }
  });
});

describe("buildApp — protected scope integration", () => {
  it("enforces auth on routes registered inside the protected scope", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;

    // The fake db always resolves to a valid row; the test verifies the wiring
    // (request reaches the route, decorations populated). Hash mismatch and
    // unknown-key paths are covered by tests/middleware/auth.test.ts.
    const fakeDb = fakeAuthDb(tenantId);

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get("/ping", async (request) => ({
          tenantId: request.tenantId,
          planTier: request.planTier,
        }));
      },
    });

    try {
      const noAuth = await app.inject({ method: "GET", url: "/ping" });
      expect(noAuth.statusCode).toBe(401);

      const withAuth = await app.inject({
        method: "GET",
        url: "/ping",
        headers: { authorization: `Bearer ${apiKey}` },
      });
      expect(withAuth.statusCode).toBe(200);
      expect(JSON.parse(withAuth.payload)).toEqual({
        tenantId,
        planTier: "pro",
      });
    } finally {
      await app.close();
    }
  });

  it("leaves health route public (registered outside the protected scope)", async () => {
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ logger: false, db: fakeDb });

    try {
      const res = await app.inject({ method: "GET", url: "/health" });
      expect(res.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });
});

describe("buildApp — inbound timeout pins", () => {
  it("sets requestTimeout=20s and headersTimeout=10s on the live server", async () => {
    const fakeDb = {} as DrizzleClient;
    const app = await buildApp({ logger: false, db: fakeDb });

    try {
      const requestTimeout = app.server.requestTimeout;
      const headersTimeout = app.server.headersTimeout;

      expect(requestTimeout).toBe(20_000);
      expect(headersTimeout).toBe(10_000);
    } finally {
      await app.close();
    }
  });

  it("pins connectionsCheckingInterval=5s in the constant — no runtime readback exists", () => {
    // Node.js does not expose a runtime readback for connectionsCheckingInterval, so we assert the constant value directly.
    expect(HTTP_SERVER_OPTIONS.connectionsCheckingInterval).toBe(5_000);
  });
});
