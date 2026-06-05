import { classify } from "../../src/upstream/classify.js";
import { describe, it, expect } from "vitest";

describe("classify", () => {
  it("classifies an upstream 4xx as client-fault, non-retryable, no breaker delta", () => {
    const expected = {
      error_class: "client-fault",
      retry_eligible: false,
      breaker_delta: 0,
    };

    const result = classify({ kind: "http_status", status: 400 });
    expect(result).toEqual(expected);
  });

  it("classifies a 2xx with an undecodable body as upstream-fault, non-retryable, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-fault",
      retry_eligible: false,
      breaker_delta: 1,
    };

    const result = classify({ kind: "decode_error" });
    expect(result).toEqual(expected);
  });

  it("classifies an upstream 5xx as retryable, terminal-class upstream-retry-exhausted, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      retry_eligible: true,
      breaker_delta: 1,
    };

    const result = classify({ kind: "http_status", status: 503 });
    expect(result).toEqual(expected);
  });

  it("classifies a 429 as retryable, upstream-retry-exhausted, breaker delta 0 (backpressure, not availability)", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      retry_eligible: true,
      breaker_delta: 0,
    };

    const result = classify({ kind: "http_status", status: 429 });
    expect(result).toEqual(expected);
  });

  it("classifies a connection failure (DNS/ECONNREFUSED) as retryable, upstream-retry-exhausted, breaker delta 1", () => {
    const expected = {
      error_class: "upstream-retry-exhausted",
      retry_eligible: true,
      breaker_delta: 1,
    };

    const result = classify({ kind: "connect_error" });
    expect(result).toEqual(expected);
  });
});
