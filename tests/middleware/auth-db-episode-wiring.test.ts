import { randomBytes } from "node:crypto";
import Fastify from "fastify";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DrizzleClient } from "../../src/db/client.js";
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

// Wiring pins for the auth DB episode alert: every catch failure feeds the
// detector (unfiltered), a resolved lookup is recovery evidence whatever the
// auth outcome, and auth_db_unavailable emits on each closed→open transition
// — a second episode in the same process summons again.
//
// vi.resetModules per cell: the detector state lives in module scope in
// auth.ts, so each cell imports a fresh copy.

// DB whose behavior is switchable per request: "fail" rejects with an
// identified cause (CONNECT_TIMEOUT keeps the vocabulary-miss alert out of
// most cells), "work" resolves to no rows — a 401, proving the DB answered.
function switchableDb(mode: { current: "fail" | "work" }): DrizzleClient {
  const limit = () =>
    mode.current === "fail"
      ? Promise.reject(
          Object.assign(new Error("write CONNECT_TIMEOUT"), {
            code: "CONNECT_TIMEOUT",
          }),
        )
      : Promise.resolve([]);
  return {
    select: () => ({
      from: () => ({
        innerJoin: () => ({ where: () => ({ limit }) }),
      }),
    }),
  } as unknown as DrizzleClient;
}

// Same ruler shape as the vocabulary-miss suite: the alert line pinned by its
// full key set, failing loudly if the payload ever grows past the allowlist.
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

interface Harness {
  logs: CapturedLog[];
  threshold: number;
  drive: (expectedStatus: number) => Promise<void>;
  close: () => Promise<unknown>;
}

// One app per cell; requests get sequential req ids (req-1, req-2, ...) so
// cells can pin WHICH request carried an alert.
async function makeHarness(db: DrizzleClient): Promise<Harness> {
  const { authPreHandler, AUTH_DB_EPISODE_THRESHOLD } =
    await import("../../src/middleware/auth.js");
  const capture = makeLogCapture();
  const app = Fastify({ logger: capture.logger });
  app.decorate("db", db);
  app.decorateRequest("tenantId", null);
  app.decorateRequest("planTier", null);
  app.decorateRequest("reqId", "");
  let requestCount = 0;
  app.addHook("onRequest", async (request) => {
    requestCount += 1;
    request.reqId = `req-${requestCount}`;
  });
  await app.register(async (scope) => {
    scope.addHook("onRequest", authPreHandler);
    scope.get("/protected", async () => ({ ok: true }));
  });
  await app.ready();

  const key = `lkey_${randomBytes(32).toString("base64url")}`;
  return {
    logs: capture.logs,
    threshold: AUTH_DB_EPISODE_THRESHOLD,
    async drive(expectedStatus: number) {
      const res = await app.inject({
        method: "GET",
        url: "/protected",
        headers: { authorization: `Bearer ${key}` },
      });
      expect(res.statusCode).toBe(expectedStatus);
    },
    close: () => app.close(),
  };
}

function episodeAlertLines(logs: CapturedLog[]): CapturedLog[] {
  return logs.filter(
    (log) =>
      log["event"] === "operational_alert" &&
      log["alert"] === "auth_db_unavailable",
  );
}

describe("auth DB episode alert — wiring", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("crossing the threshold emits auth_db_unavailable once, on the crossing request", async () => {
    const h = await makeHarness(switchableDb({ current: "fail" }));
    for (let i = 0; i < h.threshold; i++) {
      await h.drive(503);
    }
    await h.close();

    const alerts = episodeAlertLines(h.logs);
    expect(
      alerts.map((line) => line["req_id"]),
      "threshold crossing did not summon auth_db_unavailable exactly once on the crossing request",
    ).toEqual([`req-${h.threshold}`]);
    expect(
      Object.keys(alerts[0] as CapturedLog).sort(),
      "the episode alert line grew past the log allowlist (full key-set pin)",
    ).toEqual([...ALERT_LINE_KEYS].sort());
    expect((alerts[0] as CapturedLog)["level"]).toBe(50);
  });

  it("below the threshold the lane stays alert-free", async () => {
    const h = await makeHarness(switchableDb({ current: "fail" }));
    for (let i = 0; i < h.threshold - 1; i++) {
      await h.drive(503);
    }
    await h.close();

    // Positive completeness: every error-level line this lane produced is the
    // per-failure cause log — an alert would enter this list and fail it.
    const errorLevel = h.logs.filter((log) => log["level"] === 50);
    expect(errorLevel.map((log) => log["msg"])).toEqual(
      Array.from({ length: h.threshold - 1 }, () => "Auth DB lookup failed"),
    );
  });

  it("recovery then a fresh crossing summons again — no once-per-process suppression", async () => {
    const mode = { current: "fail" as "fail" | "work" };
    const h = await makeHarness(switchableDb(mode));
    for (let i = 0; i < h.threshold; i++) {
      await h.drive(503);
    }
    mode.current = "work";
    await h.drive(401); // unknown key on a healthy DB: recovery evidence
    mode.current = "fail";
    for (let i = 0; i < h.threshold; i++) {
      await h.drive(503);
    }
    await h.close();

    expect(
      episodeAlertLines(h.logs).map((line) => line["req_id"]),
      "the second episode did not summon (at-least-once per episode violated): either recovery evidence failed to close the first episode, or the emission was deduplicated once-per-process",
    ).toEqual([`req-${h.threshold}`, `req-${2 * h.threshold + 1}`]);
  });

  it("the vocabulary-miss and episode doors summon independently", async () => {
    // "boom" yields UNKNOWN cause fields AND counts toward the episode.
    const h = await makeHarness({
      select() {
        throw "boom";
      },
    } as unknown as DrizzleClient);
    for (let i = 0; i < h.threshold; i++) {
      await h.drive(503);
    }
    await h.close();

    // Full enumeration of the operational_alert lines: the once-per-process
    // miss alert on the first failure, the episode alert on the crossing.
    const alerts = h.logs.filter(
      (log) => log["event"] === "operational_alert",
    );
    expect(
      alerts.map((line) => [line["alert"], line["req_id"]]),
      "one summoning door swallowed the other: miss and episode alerts must be independent",
    ).toEqual([
      ["auth_db_cause_unknown", "req-1"],
      ["auth_db_unavailable", `req-${h.threshold}`],
    ]);
  });
});
