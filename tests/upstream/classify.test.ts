import { classify } from "../../src/upstream/classify.js";
import { describe, it, expect } from "vitest";

describe("classify", () => {
  it("classifies an upstream 4xx as client-fault, non-retryable, no breaker delta", () => {
    const expected = {
      error_class: "client-fault",
      breaker_delta: 0,
    };

    const result = classify({
      kind: "upstream_error",
      status: 400,
      body_raw: "",
    });
    expect(result).toEqual(expected);
  });

  it("classifies a 401 as client-fault too — the 4xx arm is not anchored to 400 alone", () => {
    const expected = {
      error_class: "client-fault",
      breaker_delta: 0,
    };

    const result = classify({
      kind: "upstream_error",
      status: 401,
      body_raw: "",
    });
    expect(result).toEqual(expected);
  });

  it("classifies a 2xx with an undecodable body as upstream-fault, non-retryable, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-fault",
      breaker_delta: 1,
    };

    const result = classify({ kind: "undecodable" });
    expect(result).toEqual(expected);
  });

  it("classifies an upstream 5xx as retryable, terminal-class upstream-retry-exhausted, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 1,
    };

    const result = classify({
      kind: "upstream_error",
      status: 503,
      body_raw: "",
    });
    expect(result).toEqual(expected);
  });

  it("classifies a 429 as retryable, upstream-retry-exhausted, breaker delta 0 (backpressure, not availability)", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 0,
    };

    const result = classify({
      kind: "upstream_error",
      status: 429,
      body_raw: "",
    });
    expect(result).toEqual(expected);
  });

  it("classifies a wall-clock abort as gateway-fault, non-retryable, breaker delta 1 (class and breaker are independent axes)", () => {
    const expected = {
      error_class: "gateway-fault",
      breaker_delta: 1,
    };

    const result = classify({
      kind: "aborted",
      abort_kind: "wall_clock_expired",
    });
    expect(result).toEqual(expected);
  });

  it("classifies a response-size-cap abort as upstream-fault, non-retryable, breaker delta 0 (local policy limit is not upstream unavailability)", () => {
    const expected = {
      error_class: "upstream-fault",
      breaker_delta: 0,
    };

    const result = classify({
      kind: "aborted",
      abort_kind: "response_size_cap",
    });
    expect(result).toEqual(expected);
  });

  it("classifies a possibly-post-send network failure as gateway-fault, non-retryable, breaker delta 1 (no proof the upstream never executed)", () => {
    const expected = {
      error_class: "gateway-fault",
      breaker_delta: 1,
    };

    const result = classify({ kind: "network_failed", pre_send_proven: false });
    expect(result).toEqual(expected);
  });

  it("classifies a connection failure (DNS/ECONNREFUSED) as retryable, upstream-retry-exhausted, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      breaker_delta: 1,
    };

    const result = classify({ kind: "network_failed", pre_send_proven: true });
    expect(result).toEqual(expected);
  });
});
