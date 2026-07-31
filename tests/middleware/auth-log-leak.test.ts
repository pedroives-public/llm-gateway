import { randomBytes } from "node:crypto";
import Fastify from "fastify";
import { describe, expect, it } from "vitest";
import { authPreHandler } from "../../src/middleware/auth.js";
import type { DrizzleClient } from "../../src/db/client.js";
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

// The auth middleware's DB-failure branch logs the raw driver error
// (`request.log.error({ err: error, ... })`). Every other log site in the
// gateway emits an explicit allowlist of fields; this one delegates the payload
// to whatever the driver chose to attach. These tests observe what actually
// reaches the log stream on that branch.
//
// The two error shapes below are not invented: they were reproduced against
// postgres@3.4.9 and a live PostgreSQL 17, then transcribed field-for-field.
// `Errors.connection` attaches address/port; `PostgresError` does
// `Object.assign(this, protocolFields)`, so every server-supplied field becomes
// an own enumerable property and Pino's default serializer emits all of them.

const CONNECTION_ERROR = Object.assign(
  new Error("write CONNECT_TIMEOUT 10.255.255.1:5432"),
  {
    code: "CONNECT_TIMEOUT",
    errno: "CONNECT_TIMEOUT",
    address: "10.255.255.1",
    port: 5432,
  },
);

const SERVER_ERROR = Object.assign(
  new Error('relation "api_keys" does not exist'),
  {
    name: "PostgresError",
    severity_local: "ERROR",
    severity: "ERROR",
    code: "42P01",
    position: "15",
    file: "parse_relation.c",
    line: "1449",
    routine: "parserOpenTable",
  },
);

function throwingDb(error: Error): DrizzleClient {
  return {
    select() {
      throw error;
    },
  } as unknown as DrizzleClient;
}

// Drive one authenticated request against a DB that fails, and return the log
// line the auth middleware emitted for that failure.
async function authFailureLog(error: Error): Promise<CapturedLog> {
  const capture = makeLogCapture();
  const app = Fastify({ logger: capture.logger });
  app.decorate("db", throwingDb(error));
  app.decorateRequest("tenantId", null);
  app.decorateRequest("planTier", null);

  await app.register(async (scope) => {
    scope.addHook("onRequest", authPreHandler);
    scope.get("/protected", async () => ({ ok: true }));
  });
  await app.ready();

  const key = `lkey_${randomBytes(32).toString("base64url")}`;
  const res = await app.inject({
    method: "GET",
    url: "/protected",
    headers: { authorization: `Bearer ${key}` },
  });
  await app.close();

  expect(res.statusCode).toBe(503);

  const failureLog = capture.logs.find(
    (log) => log.msg === "Auth DB lookup failed",
  );
  expect(
    failureLog,
    "auth middleware did not log the DB failure",
  ).toBeDefined();
  return failureLog as CapturedLog;
}

// Each assertion names one thing the probe found in this line: a slice of the
// caller's key, the database address, and PostgreSQL internals. Serializing the
// whole record catches a value at any nesting depth, so a field moving deeper
// into the payload cannot slip past.
function assertAuthFailureLogIsSafe(log: CapturedLog): void {
  const line = JSON.stringify(log);
  expect(line).not.toContain("lkey_");
  expect(line).not.toContain("10.255.255.1");
  expect(line).not.toContain("parse_relation.c");
}

describe("auth middleware — what the DB-failure branch writes to the log", () => {
  it("connection failure (address/port attached by the driver)", async () => {
    assertAuthFailureLogIsSafe(await authFailureLog(CONNECTION_ERROR));
  });

  it("server-side failure (PostgresError protocol fields)", async () => {
    assertAuthFailureLogIsSafe(await authFailureLog(SERVER_ERROR));
  });
});
