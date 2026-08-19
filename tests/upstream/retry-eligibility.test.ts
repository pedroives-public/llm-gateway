import { describe, it, expect } from "vitest";
import {
  isRetryEligible,
  retryAfterMs,
} from "../../src/upstream/retry-eligibility.js";
import type { ErrorOutcome, Outcome } from "../../src/upstream/outcome.js";

// Retry-eligibility is default-deny: retry only when the outcome PROVES the
// attempt did not cross the inference commit point (acceptance/start of
// execution, with its quota consumption and billing). The complete allowlist
// of eligible outcomes is:
//
//   { kind: "network_failed", pre_send_proven: true }
//
// and nothing else — the per-status derivation lives in retry-eligibility.ts.
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
      name: "upstream 503 — default-deny: refusal semantics carry no commit-point proof without an adapter contract",
      outcome: { kind: "upstream_error", status: 503, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 500 — default-deny: origin error does not prove the attempt was not accepted",
      outcome: { kind: "upstream_error", status: 500, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 502 — default-deny: intermediary speech, the origin's state is unknowable",
      outcome: { kind: "upstream_error", status: 502, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 504 — default-deny: the origin may complete (and bill) after the intermediary gave up",
      outcome: { kind: "upstream_error", status: 504, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 529 (nonstandard, e.g. Anthropic overloaded) — default-deny: unknown 5xx carries no proof",
      outcome: { kind: "upstream_error", status: 529, body_raw: "" },
      eligible: false,
    },
    {
      name: "upstream 429 — default-deny: refusal semantics carry no commit-point proof without an adapter contract",
      outcome: { kind: "upstream_error", status: 429, body_raw: "" },
      eligible: false,
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
      name: "upstream 429 with the legitimate rate-limit code — default-deny: a benign body does not resurrect eligibility",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: { code: "rate_limit_exceeded" } }),
      },
      eligible: false,
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
