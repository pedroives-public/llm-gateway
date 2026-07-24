import { classify } from "../../src/upstream/classify.js";
import { describe, it, expect, vi } from "vitest";

const makeLog = () => ({ error: vi.fn() });
describe("classify", () => {
  it("classifies an upstream 4xx as client-fault, non-retryable, no breaker delta", () => {
    const expected = {
      error_class: "client-fault",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 400,
        body_raw: "",
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies an upstream 401 as upstream-auth-failure, breaker delta 1 (deployment credential rejected)", () => {
    const expected = {
      error_class: "upstream-auth-failure",
      breaker_delta: 1,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 401,
        body_raw: "",
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies an upstream 403 as upstream-access-denied, breaker delta 0 (consumer-influencible)", () => {
    const expected = {
      error_class: "upstream-access-denied",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 403,
        body_raw: "",
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies a 404 as client-fault too — the 4xx arm is not anchored to 400 alone", () => {
    const expected = {
      error_class: "client-fault",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 404,
        body_raw: "",
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies a 2xx with an undecodable body as upstream-fault, non-retryable, breaker delta 1", () => {
    const log = makeLog();
    const expected = {
      error_class: "upstream-fault",
      breaker_delta: 1,
    };

    const result = classify({ kind: "undecodable" }, log, "req-1");
    expect(result).toEqual(expected);
    expect(log.error).toHaveBeenCalledWith({
      req_id: "req-1",
      cause_code: null,
      cause_name: null,
    });
  });

  it("classifies an upstream 5xx as retryable, terminal-class upstream-retry-exhausted, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 1,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 503,
        body_raw: "",
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies a 429 as retryable, upstream-retry-exhausted, breaker delta 0 (backpressure, not availability)", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 429,
        body_raw: "",
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies a 429 carrying insufficient_quota as upstream-quota-exhausted, breaker delta 0 (body-derived, never breaker)", () => {
    const expected = {
      error_class: "upstream-quota-exhausted",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: { code: "insufficient_quota" } }),
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("keeps a 429 with the legitimate rate-limit code on upstream-retry-exhausted — quota discrimination does not widen", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: { code: "rate_limit_exceeded" } }),
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies a wall-clock abort as gateway-fault, non-retryable, breaker delta 1 (class and breaker are independent axes)", () => {
    const log = makeLog();
    const expected = {
      error_class: "gateway-fault",
      breaker_delta: 1,
    };

    const result = classify(
      {
        kind: "aborted",
        abort_kind: "wall_clock_expired",
      },
      log,
      "req-1",
    );
    expect(result).toEqual(expected);
    expect(log.error).not.toHaveBeenCalled();
  });

  it("classifies a response-size-cap abort as upstream-fault, non-retryable, breaker delta 0 (local policy limit is not upstream unavailability)", () => {
    const log = makeLog();
    const expected = {
      error_class: "upstream-fault",
      breaker_delta: 0,
    };

    const result = classify(
      {
        kind: "aborted",
        abort_kind: "response_size_cap",
      },
      log,
      "req-1",
    );
    expect(result).toEqual(expected);
    expect(log.error).not.toHaveBeenCalled();
  });

  it("classifies a possibly-post-send network failure as gateway-fault, non-retryable, breaker delta 1 (no proof the upstream never executed)", () => {
    const expected = {
      error_class: "gateway-fault",
      breaker_delta: 1,
    };

    const result = classify(
      {
        kind: "network_failed",
        pre_send_proven: false,
      },
      makeLog(),
      "req-1",
    );
    expect(result).toEqual(expected);
  });

  it("classifies a connection failure (DNS/ECONNREFUSED) as retryable, upstream-retry-exhausted, breaker delta 1", () => {
    const log = makeLog();
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 1,
    };

    const result = classify(
      {
        kind: "network_failed",
        pre_send_proven: true,
        cause_code: "ECONNREFUSED",
        cause_name: "Error",
      },
      log,
      "req-1",
    );
    expect(result).toEqual(expected);
    expect(log.error).toHaveBeenCalledWith({
      req_id: "req-1",
      cause_code: "ECONNREFUSED",
      cause_name: "Error",
    });
  });

  it("logs null cause fields for a recognized network_failed that carries no cause (the ?? null fallback)", () => {
    const log = makeLog();
    classify({ kind: "network_failed", pre_send_proven: true }, log, "req-1");
    expect(log.error).toHaveBeenCalledWith({
      req_id: "req-1",
      cause_code: null,
      cause_name: null,
    });
  });

  it("logs the raw upstream cause at error level before returning the label, with null cause fields for a status error", () => {
    const log = { error: vi.fn() };
    classify(
      {
        kind: "upstream_error",
        status: 503,
        body_raw: "X",
      },
      log,
      "req-1",
    );
    expect(log.error).toHaveBeenCalledWith({
      req_id: "req-1",
      cause_code: null,
      cause_name: null,
    });
  });
});
