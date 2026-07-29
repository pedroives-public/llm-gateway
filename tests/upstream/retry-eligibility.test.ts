import { describe, it, expect } from "vitest";
import {
  isRetryEligible,
  retryAfterMs,
} from "../../src/upstream/retry-eligibility.js";
import type { ErrorOutcome, Outcome } from "../../src/upstream/outcome.js";

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
      name: "upstream 429 carrying insufficient_quota — quota exhaustion, not retryable",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: { code: "insufficient_quota" } }),
      },
      eligible: false,
    },
    {
      name: "upstream 429 with the legitimate rate-limit code — still retry-eligible",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: { code: "rate_limit_exceeded" } }),
      },
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
    {
      name: "blocked redirect — structurally not retryable (3xx repeats until config changes)",
      outcome: { kind: "redirect_blocked" },
      eligible: false,
    },
  ];

  it.each(cases)("$name", ({ outcome, eligible }) => {
    expect(isRetryEligible(outcome)).toBe(eligible);
  });
});

// Retry-After has temporal authority only on 429 (RFC 6585 §4) and 503
// (RFC 9110 §10.2.3); any other status yields null and the caller jitters.
// Living in this single source, the whitelist keeps the sleep, the
// skipped_budget disposition, and the budget-skip terminal aligned — an
// untrusted upstream header commands none of them.
describe("retryAfterMs", () => {
  const cases: ReadonlyArray<{
    name: string;
    outcome: Outcome;
    expected: number | null;
  }> = [
    {
      name: "429 with Retry-After: 25 — honored, delta-seconds converted to ms",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: "",
        retry_after: "25",
      },
      expected: 25_000,
    },
    {
      name: "503 with Retry-After: 7 — honored, delta-seconds converted to ms",
      outcome: {
        kind: "upstream_error",
        status: 503,
        body_raw: "",
        retry_after: "7",
      },
      expected: 7_000,
    },
    {
      name: "500 with Retry-After: 25 — no temporal authority, header ignored",
      outcome: {
        kind: "upstream_error",
        status: 500,
        body_raw: "",
        retry_after: "25",
      },
      expected: null,
    },
    {
      name: "502 with Retry-After: 25 — no temporal authority, header ignored",
      outcome: {
        kind: "upstream_error",
        status: 502,
        body_raw: "",
        retry_after: "25",
      },
      expected: null,
    },
    {
      name: "504 with Retry-After: 25 — no temporal authority, header ignored",
      outcome: {
        kind: "upstream_error",
        status: 504,
        body_raw: "",
        retry_after: "25",
      },
      expected: null,
    },
    {
      name: "429 with absent Retry-After — null (validity rule unchanged by the whitelist)",
      outcome: { kind: "upstream_error", status: 429, body_raw: "" },
      expected: null,
    },
    {
      name: "429 with Retry-After: 0 — zero is not a usable wait, even whitelisted",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: "",
        retry_after: "0",
      },
      expected: null,
    },
    {
      name: "network_failed — kind gate holds: only upstream errors carry the header",
      outcome: { kind: "network_failed", pre_send_proven: true },
      expected: null,
    },
  ];

  it.each(cases)("$name", ({ outcome, expected }) => {
    expect(retryAfterMs(outcome)).toBe(expected);
  });
});
