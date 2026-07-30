import { beforeEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import {
  type ReqStartPayload,
  type ReqCompletePayload,
  type StreamFirstTokenPayload,
  type StreamDonePayload,
  type CbStateChangePayload,
  ALERT_NAMES,
  alertFor,
  type ErrorClass,
  type AlertName,
} from "../../src/observability/events.js";

describe("wasColdStart", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("returns true before any request completes", async () => {
    const { wasColdStart } = await import("../../src/observability/events.js");
    expect(wasColdStart()).toBe(true);
  });

  it("returns false after _markFirstRequestCompleted", async () => {
    const { wasColdStart, _markFirstRequestCompleted } =
      await import("../../src/observability/events.js");
    expect(wasColdStart()).toBe(true);
    _markFirstRequestCompleted();
    expect(wasColdStart()).toBe(false);
  });

  it("stays false on repeated _markFirstRequestCompleted calls", async () => {
    const { wasColdStart, _markFirstRequestCompleted } =
      await import("../../src/observability/events.js");
    _markFirstRequestCompleted();
    _markFirstRequestCompleted();
    expect(wasColdStart()).toBe(false);
  });
});

describe("event emitters — field shape", () => {
  let logCalls: object[];
  let mockLog: { info: (obj: object) => void };

  beforeEach(() => {
    vi.resetModules();
    logCalls = [];
    mockLog = { info: (obj: object) => logCalls.push(obj) };
  });

  it("emitReqStart emits all required fields and no extras", async () => {
    const { emitReqStart } = await import("../../src/observability/events.js");
    const payload: ReqStartPayload = {
      req_id: "019e6ef2-5065-74d9-ab04-834a39c6e4a9",
      route: "/v1/chat/completions",
      tenant_id: "t1",
      plan_tier: "pro",
      stream: false,
      idempotency_key_present: false,
      was_cold_start: true,
    };
    emitReqStart(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "req_start", ...payload });
  });

  it("emitReqComplete emits all required fields and no extras", async () => {
    const { emitReqComplete } =
      await import("../../src/observability/events.js");
    const payload: ReqCompletePayload = {
      req_id: "019e6ef2-5065-74d9-ab04-834a39c6e4a9",
      status: 200,
      error_class: null,
      stream: false,
      duration_ms: 120,
      upstream_duration_ms: 100,
      gateway_overhead_ms: 20,
      attempts: 1,
      retry_disposition: "ineligible",
    };
    emitReqComplete(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "req_complete", ...payload });
  });

  it("emitReqComplete calls _markFirstRequestCompleted", async () => {
    const { emitReqComplete, wasColdStart } =
      await import("../../src/observability/events.js");
    expect(wasColdStart()).toBe(true);
    emitReqComplete(mockLog, {
      req_id: "x",
      status: 200,
      error_class: null,
      stream: false,
      duration_ms: 10,
      upstream_duration_ms: 8,
      gateway_overhead_ms: 2,
      attempts: 1,
      retry_disposition: "ineligible",
    });
    expect(wasColdStart()).toBe(false);
  });

  it("emitStreamFirstToken emits all required fields and no extras", async () => {
    const { emitStreamFirstToken } =
      await import("../../src/observability/events.js");
    const payload: StreamFirstTokenPayload = {
      req_id: "019e6ef2-5065-74d9-ab04-834a39c6e4a9",
      ttft_ms: 350,
      was_cold_start: true,
    };
    emitStreamFirstToken(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "stream_first_token", ...payload });
  });

  it("emitStreamDone emits all required fields and no extras (completed)", async () => {
    const { emitStreamDone } =
      await import("../../src/observability/events.js");
    const payload: StreamDonePayload = {
      req_id: "019e6ef2-5065-74d9-ab04-834a39c6e4a9",
      completed: true,
      total_duration_ms: 2000,
      upstream_duration_ms: 1900,
      gateway_overhead_ms: 100,
      attempts: 1,
      error_class: null,
    };
    emitStreamDone(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "stream_done", ...payload });
  });

  it("emitStreamDone calls _markFirstRequestCompleted", async () => {
    const { emitStreamDone, wasColdStart } =
      await import("../../src/observability/events.js");
    expect(wasColdStart()).toBe(true);
    emitStreamDone(mockLog, {
      req_id: "x",
      completed: true,
      total_duration_ms: 10,
      upstream_duration_ms: 8,
      gateway_overhead_ms: 2,
      attempts: 1,
      error_class: null,
    });
    expect(wasColdStart()).toBe(false);
  });

  it("emitCbStateChange emits all required fields and no extras", async () => {
    const { emitCbStateChange } =
      await import("../../src/observability/events.js");
    const payload: CbStateChangePayload = {
      from: "CLOSED",
      to: "OPEN",
      failure_count: 5,
      window_start_ms: 1748000000000,
    };
    emitCbStateChange(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "cb_state_change", ...payload });
  });

  it("req_id propagates across events for the same request", async () => {
    const { emitReqStart, emitReqComplete } =
      await import("../../src/observability/events.js");
    const req_id = "019e6ef2-5065-74d9-ab04-834a39c6e4a9";
    emitReqStart(mockLog, {
      req_id,
      route: "/v1/chat/completions",
      tenant_id: "t1",
      plan_tier: "pro",
      stream: false,
      idempotency_key_present: false,
      was_cold_start: true,
    });
    emitReqComplete(mockLog, {
      req_id,
      status: 200,
      error_class: null,
      stream: false,
      duration_ms: 120,
      upstream_duration_ms: 100,
      gateway_overhead_ms: 20,
      attempts: 1,
      retry_disposition: "ineligible",
    });
    const start = logCalls[0] as Record<string, unknown>;
    const complete = logCalls[1] as Record<string, unknown>;
    expect(start["req_id"]).toBe(req_id);
    expect(complete["req_id"]).toBe(req_id);
  });

  it("was_cold_start is true on first request events, false on subsequent", async () => {
    const { emitReqStart, emitReqComplete, wasColdStart } =
      await import("../../src/observability/events.js");

    const firstColdStart = wasColdStart();
    emitReqStart(mockLog, {
      req_id: "req1",
      route: "/v1/chat/completions",
      tenant_id: "t1",
      plan_tier: "pro",
      stream: false,
      idempotency_key_present: false,
      was_cold_start: firstColdStart,
    });
    emitReqComplete(mockLog, {
      req_id: "req1",
      status: 200,
      error_class: null,
      stream: false,
      duration_ms: 10,
      upstream_duration_ms: 8,
      gateway_overhead_ms: 2,
      attempts: 1,
      retry_disposition: "ineligible",
    });

    const secondColdStart = wasColdStart();
    emitReqStart(mockLog, {
      req_id: "req2",
      route: "/v1/chat/completions",
      tenant_id: "t1",
      plan_tier: "pro",
      stream: false,
      idempotency_key_present: false,
      was_cold_start: secondColdStart,
    });

    const firstStart = logCalls[0] as Record<string, unknown>;
    const secondStart = logCalls[2] as Record<string, unknown>;
    expect(firstStart["was_cold_start"]).toBe(true);
    expect(secondStart["was_cold_start"]).toBe(false);
  });
});

describe("emitOperationalAlert", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it.each(ALERT_NAMES)(
    "emits exactly-once per process for %s",
    async (alert) => {
      const { emitOperationalAlert } =
        await import("../../src/observability/events.js");

      const errorCalls: object[] = [];
      const mockLog = { error: (obj: object) => errorCalls.push(obj) };

      emitOperationalAlert(mockLog, alert, { req_id: "r-1" });
      emitOperationalAlert(mockLog, alert, { req_id: "r-2" });

      expect(errorCalls.length).toBe(1);
      expect(errorCalls[0]).toEqual({
        event: "operational_alert",
        alert,
        req_id: "r-1",
      });
    },
  );

  it("emits different alerts independently", async () => {
    const { emitOperationalAlert } =
      await import("../../src/observability/events.js");

    const errorCalls: object[] = [];
    const mockLog = { error: (obj: object) => errorCalls.push(obj) };

    for (const alert of ALERT_NAMES) {
      emitOperationalAlert(mockLog, alert, { req_id: `r-${alert}` });
    }

    expect(errorCalls.length).toEqual(ALERT_NAMES.length);
    for (const alert of ALERT_NAMES) {
      expect(errorCalls).toContainEqual({
        event: "operational_alert",
        alert,
        req_id: `r-${alert}`,
      });
    }

    for (const alert of ALERT_NAMES) {
      emitOperationalAlert(mockLog, alert, { req_id: `again-${alert}` });
    }

    // no re-summon: the count is frozen at one line per name
    expect(errorCalls.length).toEqual(ALERT_NAMES.length);
  });
});

describe("alertFor", () => {
  // The mapping pairs and the silent list must jointly cover ErrorClass: the
  // type lock makes the typecheck fail whenever a future ErrorClass is left
  // out of both, forcing every new class to declare itself summoning or
  // silent here. Expected values are literal on purpose — a computed oracle
  // would share its rule with the implementation.
  const MAPPING_PAIRS = [
    ["upstream-auth-failure", "upstream_auth_failure"],
    ["upstream-quota-exhausted", "upstream_quota_exhausted"],
    ["upstream-redirect-blocked", "upstream_redirect_blocked"],
  ] as const satisfies readonly [ErrorClass, AlertName][];

  const SILENT_CLASSES = [
    "client-fault",
    "gateway-fault",
    "upstream-fault",
    "upstream-retry-exhausted",
    "upstream-access-denied",
  ] as const satisfies readonly ErrorClass[];

  type Covered =
    | (typeof MAPPING_PAIRS)[number][0]
    | (typeof SILENT_CLASSES)[number];
  type Missing = Exclude<ErrorClass, Covered>;
  expectTypeOf<Missing>().toEqualTypeOf<never>();

  it.each(MAPPING_PAIRS)("maps %s to %s", (errorClass, expectedAlert) => {
    expect(alertFor(errorClass)).toBe(expectedAlert);
  });

  it.each(SILENT_CLASSES)("stays silent for %s", (errorClass) => {
    expect(alertFor(errorClass)).toBeNull();
  });
});
