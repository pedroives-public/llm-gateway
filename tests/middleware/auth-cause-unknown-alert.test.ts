import { randomBytes } from "node:crypto";
import Fastify from "fastify";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DrizzleClient } from "../../src/db/client.js";
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

// The auth DB catch logs only the {cause_code, cause_name} pair. When either
// field falls back to UNKNOWN, the thrown error's shape is outside the
// extraction vocabulary — a condition the operator must hear about, so the
// catch summons the auth_db_cause_unknown alert directly: there is no
// ErrorClass on this branch for alertFor to map.
//
// vi.resetModules per cell: the alert's once-per-process flag lives in module
// state, so each cell imports a fresh copy of auth + events.

const TEST_REQ_ID = "019e6ef2-5065-74d9-ab04-834a39c6e4a9";

function throwingDb(error: unknown): DrizzleClient {
  return {
    select() {
      throw error;
    },
  } as unknown as DrizzleClient;
}

// Drive one well-formed-key request against a DB that fails and return every
// line the process logged for it.
async function driveAuthDbFailure(error: unknown): Promise<CapturedLog[]> {
  const { authPreHandler } = await import("../../src/middleware/auth.js");
  const capture = makeLogCapture();
  const app = Fastify({ logger: capture.logger });
  app.decorate("db", throwingDb(error));
  app.decorateRequest("tenantId", null);
  app.decorateRequest("planTier", null);
  app.decorateRequest("reqId", "");
  app.addHook("onRequest", async (request) => {
    request.reqId = TEST_REQ_ID;
  });

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
  return capture.logs;
}

function alertLines(logs: CapturedLog[]): CapturedLog[] {
  return logs.filter((log) => log["event"] === "operational_alert");
}

// Same ruler shape as the leak suite: the alert line pinned by its full key
// set, so the cell fails loudly if the payload ever grows past the log
// allowlist. No msg key: the emitter logs a pure structured line.
const ALERT_LINE_KEYS = [
  "alert",
  "event",
  "hostname",
  "level",
  "pid",
  "reqId",
  "req_id",
  "time",
];

describe("auth DB catch — vocabulary-miss alert", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("summons the operator when the thrown shape yields no cause at all", async () => {
    const logs = await driveAuthDbFailure("boom");

    const alerts = alertLines(logs);
    expect(
      alerts.length,
      "vocabulary miss did not summon the operator (auth_db_cause_unknown)",
    ).toBe(1);

    const line = alerts[0] as CapturedLog;
    expect(Object.keys(line).sort()).toEqual([...ALERT_LINE_KEYS].sort());
    expect(line["alert"]).toBe("auth_db_cause_unknown");
    expect(line["req_id"]).toBe(TEST_REQ_ID);
    expect(line["level"]).toBe(50);
  });

  it("summons the operator when only one identifying field is missing", async () => {
    // code extracted, name unidentifiable: a partial miss is still a form
    // the vocabulary failed to identify
    const logs = await driveAuthDbFailure({ code: "57014" });

    const alerts = alertLines(logs);
    expect(
      alerts.length,
      "partial vocabulary miss did not summon the operator (auth_db_cause_unknown)",
    ).toBe(1);
    expect((alerts[0] as CapturedLog)["alert"]).toBe("auth_db_cause_unknown");
  });

  it("a fully identified cause emits exactly the failure line at error level", async () => {
    const identified = Object.assign(new Error("write CONNECT_TIMEOUT"), {
      code: "CONNECT_TIMEOUT",
    });
    const logs = await driveAuthDbFailure(identified);

    // Positive completeness: enumerate EVERY error-level line this branch
    // produced. A summoning here would enter this list and fail the ruler.
    const errorLevel = logs.filter((log) => log["level"] === 50);
    expect(errorLevel.map((log) => log["msg"])).toEqual([
      "Auth DB lookup failed",
    ]);
  });
});
