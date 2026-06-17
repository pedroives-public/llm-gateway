import { describe, it, expect } from "vitest";
import { isRetryEligible } from "../../src/upstream/retry-eligibility.js";
import type { ErrorOutcome } from "../../src/upstream/outcome.js";

// Pure retry-eligibility predicate extracted from classify (design step 1, M-1). It is the SOLE reader of retry-eligibility; the verdicts mirror the
// `retry_eligible` column the classifier used to return, so retry behavior is
// unchanged by the extraction. The predicate reads only kind/status/pre_send;
// it never inspects cause fields or logs.
describe("isRetryEligible", () => {
  const cases: ReadonlyArray<{
    name: string;
    outcome: ErrorOutcome;
    eligible: boolean;
  }> = [
    {
      name: "upstream 4xx (400) — client fault, not retryable",
      outcome: { kind: "upstream_error", status: 400, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 4xx (401) — the 4xx arm is not anchored to 400 alone",
      outcome: { kind: "upstream_error", status: 401, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 5xx (503) — retry-eligible",
      outcome: { kind: "upstream_error", status: 503, body_raw: "" },
      eligible: true,
    },
    {
      name: "upstream 429 — retry-eligible (backpressure)",
      outcome: { kind: "upstream_error", status: 429, body_raw: "" },
      eligible: true,
    },
    {
      name: "undecodable 2xx body — not retryable",
      outcome: { kind: "undecodable" },
      eligible: false,
    },
    {
      name: "wall-clock abort — gateway terminal, not retryable",
      outcome: { kind: "aborted", abort_kind: "wall_clock_expired" },
      eligible: false,
    },
    {
      name: "response-size-cap abort — gateway terminal, not retryable",
      outcome: { kind: "aborted", abort_kind: "response_size_cap" },
      eligible: false,
    },
    {
      name: "pre-send-proven network failure (DNS/ECONNREFUSED) — retryable",
      outcome: { kind: "network_failed", pre_send_proven: true },
      eligible: true,
    },
    {
      name: "not-proven-pre-send network failure — not retryable (idempotency-unsafe)",
      outcome: { kind: "network_failed", pre_send_proven: false },
      eligible: false,
    },
  ];

  it.each(cases)("$name", ({ outcome, eligible }) => {
    expect(isRetryEligible(outcome)).toBe(eligible);
  });
});
