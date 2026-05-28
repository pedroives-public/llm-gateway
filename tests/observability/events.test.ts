import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ReqStartPayload,
  ReqCompletePayload,
  StreamFirstTokenPayload,
  StreamDonePayload,
  CbStateChangePayload,
} from "../../src/observability/events.js";

describe("wasColdStart", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("returns true before any request completes", async () => {
    const { wasColdStart } = await import(
      "../../src/observability/events.js"
    );
    expect(wasColdStart()).toBe(true);
  });

  it("returns false after _markFirstRequestCompleted", async () => {
    const { wasColdStart, _markFirstRequestCompleted } = await import(
      "../../src/observability/events.js"
    );
    expect(wasColdStart()).toBe(true);
    _markFirstRequestCompleted();
    expect(wasColdStart()).toBe(false);
  });

  it("stays false on repeated _markFirstRequestCompleted calls", async () => {
    const { wasColdStart, _markFirstRequestCompleted } = await import(
      "../../src/observability/events.js"
    );
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
    const { emitReqComplete } = await import(
      "../../src/observability/events.js"
    );
    const payload: ReqCompletePayload = {
      req_id: "019e6ef2-5065-74d9-ab04-834a39c6e4a9",
      status: 200,
      error_class: null,
      duration_ms: 120,
      upstream_duration_ms: 100,
      gateway_overhead_ms: 20,
      attempts: 1,
    };
    emitReqComplete(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "req_complete", ...payload });
  });

  it("emitReqComplete calls _markFirstRequestCompleted", async () => {
    const { emitReqComplete, wasColdStart } = await import(
      "../../src/observability/events.js"
    );
    expect(wasColdStart()).toBe(true);
    emitReqComplete(mockLog, {
      req_id: "x",
      status: 200,
      error_class: null,
      duration_ms: 10,
      upstream_duration_ms: 8,
      gateway_overhead_ms: 2,
      attempts: 1,
    });
    expect(wasColdStart()).toBe(false);
  });

  it("emitStreamFirstToken emits all required fields and no extras", async () => {
    const { emitStreamFirstToken } = await import(
      "../../src/observability/events.js"
    );
    const payload: StreamFirstTokenPayload = {
      req_id: "019e6ef2-5065-74d9-ab04-834a39c6e4a9",
      ttft_ms: 350,
      was_cold_start: true,
    };
    emitStreamFirstToken(mockLog, payload);
    expect(logCalls[0]).toEqual({ event: "stream_first_token", ...payload });
  });

  it("emitStreamDone emits all required fields and no extras (completed)", async () => {
    const { emitStreamDone } = await import(
      "../../src/observability/events.js"
    );
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
    const { emitStreamDone, wasColdStart } = await import(
      "../../src/observability/events.js"
    );
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
    const { emitCbStateChange } = await import(
      "../../src/observability/events.js"
    );
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
    const { emitReqStart, emitReqComplete } = await import(
      "../../src/observability/events.js"
    );
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
      duration_ms: 120,
      upstream_duration_ms: 100,
      gateway_overhead_ms: 20,
      attempts: 1,
    });
    const start = logCalls[0] as Record<string, unknown>;
    const complete = logCalls[1] as Record<string, unknown>;
    expect(start["req_id"]).toBe(req_id);
    expect(complete["req_id"]).toBe(req_id);
  });

  it("was_cold_start is true on first request events, false on subsequent", async () => {
    const { emitReqStart, emitReqComplete, wasColdStart } = await import(
      "../../src/observability/events.js"
    );

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
      duration_ms: 10,
      upstream_duration_ms: 8,
      gateway_overhead_ms: 2,
      attempts: 1,
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
