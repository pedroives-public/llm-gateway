import { describe, it, expect } from "vitest";
import { isInsufficientQuota } from "../../src/upstream/quota.js";
import type { Outcome } from "../../src/upstream/outcome.js";

// Behavior cells for isInsufficientQuota. The positive fixture uses the
// documented-only body shape (evidence debt): replace it with the verbatim
// body of the first real quota 429 captured in staging or production.
describe("isInsufficientQuota", () => {
  const cases: ReadonlyArray<{
    name: string;
    outcome: Outcome;
    expected: boolean;
  }> = [
    {
      name: "429 with documented insufficient_quota body — recognized",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({
          error: {
            message:
              "You exceeded your current quota, please check your plan and billing details.",
            type: "insufficient_quota",
            code: "insufficient_quota",
          },
        }),
      },
      expected: true,
    },
    {
      name: "429 with a non-JSON body (HTML error page) — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: "<html>502 Bad Gateway</html>",
      },
      expected: false,
    },
    {
      name: "429 with valid JSON but no error field — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: "{}",
      },
      expected: false,
    },
    {
      name: "429 with valid JSON but error is not an object — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: "insufficient_quota" }),
      },
      expected: false,
    },
    {
      name: "429 with valid JSON but no code field — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({
          error: {
            message: "...",
          },
        }),
      },
      expected: false,
    },
    {
      name: "429 with the legitimate rate-limit code (rate_limit_exceeded) — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({
          error: {
            code: "rate_limit_exceeded",
          },
        }),
      },
      expected: false,
    },
    {
      name: "429 with valid JSON but with non-string code — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({
          error: {
            code: 42,
          },
        }),
      },
      expected: false,
    },
    {
      name: "503 carrying a perfect insufficient_quota body — status outside the subfamily, not quota",
      outcome: {
        kind: "upstream_error",
        status: 503,
        body_raw: JSON.stringify({
          error: {
            message:
              "You exceeded your current quota, please check your plan and billing details.",
            type: "insufficient_quota",
            code: "insufficient_quota",
          },
        }),
      },
      expected: false,
    },
    {
      name: "non-upstream_error outcome (undecodable) — no body to inspect, not quota",
      outcome: { kind: "undecodable" },
      expected: false,
    },
    {
      name: "429 with the JSON literal null as body — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: "null",
      },
      expected: false,
    },
    {
      name: "429 with an error field that is null — fail-open, not quota",
      outcome: {
        kind: "upstream_error",
        status: 429,
        body_raw: JSON.stringify({ error: null }),
      },
      expected: false,
    },
  ];

  it.each(cases)("$name", ({ outcome, expected }) => {
    expect(isInsufficientQuota(outcome)).toBe(expected);
  });
});
