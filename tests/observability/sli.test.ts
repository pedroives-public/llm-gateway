import { describe, it, expect } from "vitest";
import type {
  ErrorClass,
  ReqCompletePayload,
  StreamDonePayload,
  StreamFirstTokenPayload,
} from "../../src/observability/events.js";

// Reference implementations of the six SLI formulas, evaluated against
// synthetic event populations. Expected values are derived arithmetically
// from the construction parameters (counts and planted latencies) — never by
// re-running the formula under test — so a filter or merge bug diverges from
// the expectation instead of reproducing it. Quantiles use nearest-rank; the
// contract's ±0.5% tolerance covers method drift at production sample sizes,
// while these small populations verify population semantics (filters and the
// req_complete + stream_done merge), not the interpolation method.

type EventSet = {
  reqComplete: ReqCompletePayload[];
  streamDone: StreamDonePayload[];
  streamFirstToken: StreamFirstTokenPayload[];
};

const range = (n: number): number[] => Array.from({ length: n }, (_, j) => j);

// Identity and retry fields are irrelevant to every SLI formula; the builders
// fix them so each synthetic row stays a one-liner at the call site.
function reqComplete(
  errorClass: ErrorClass | null,
  stream: boolean,
  durationMs: number,
  overheadMs: number,
): ReqCompletePayload {
  return {
    req_id: "r",
    status: errorClass === null ? 200 : 502,
    error_class: errorClass,
    stream,
    duration_ms: durationMs,
    upstream_duration_ms: Math.max(0, durationMs - overheadMs),
    gateway_overhead_ms: overheadMs,
    attempts: 1,
    retry_disposition: "ineligible",
  };
}

function streamDone(
  errorClass: ErrorClass | null,
  completed: boolean,
  overheadMs: number,
): StreamDonePayload {
  return {
    req_id: "r",
    completed,
    total_duration_ms: 1_000,
    upstream_duration_ms: Math.max(0, 1_000 - overheadMs),
    gateway_overhead_ms: overheadMs,
    attempts: 1,
    error_class: errorClass,
  };
}

function streamFirstToken(
  ttftMs: number,
  wasColdStart: boolean,
): StreamFirstTokenPayload {
  return { req_id: "r", ttft_ms: ttftMs, was_cold_start: wasColdStart };
}

// Nearest-rank quantile: the p-th quantile of N samples is the ceil(p*N)-th
// smallest, so expectations stay hand-derivable in closed form.
function quantileNearestRank(p: number, values: readonly number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const picked = sorted[Math.ceil(p * sorted.length) - 1];
  if (picked === undefined) {
    throw new Error("quantile over an empty population");
  }
  return picked;
}

// ── The six SLI formulas ───────────────────────────────────────────────

function gatewayOverheadP99(set: EventSet): number {
  return quantileNearestRank(0.99, [
    ...set.reqComplete.map((e) => e.gateway_overhead_ms),
    ...set.streamDone.map((e) => e.gateway_overhead_ms),
  ]);
}

function ttftWarmP95(set: EventSet): number {
  return quantileNearestRank(
    0.95,
    set.streamFirstToken.filter((e) => !e.was_cold_start).map((e) => e.ttft_ms),
  );
}

function ttftColdP99(set: EventSet): number {
  return quantileNearestRank(
    0.99,
    set.streamFirstToken.filter((e) => e.was_cold_start).map((e) => e.ttft_ms),
  );
}

function completionRate(set: EventSet): number {
  const successes = set.streamDone.filter((e) => e.completed);
  const terminals = set.streamDone.filter(
    (e) => e.error_class !== "client-fault",
  );
  return successes.length / terminals.length;
}

function totalResponseTimeNonstreamP95(set: EventSet): number {
  return quantileNearestRank(
    0.95,
    set.reqComplete
      .filter((e) => !e.stream && e.error_class === null)
      .map((e) => e.duration_ms),
  );
}

function requestSuccessRate(set: EventSet): number {
  const terminals = [...set.reqComplete, ...set.streamDone];
  const successes = terminals.filter(
    (e) => e.error_class === null || e.error_class === "client-fault",
  );
  return successes.length / terminals.length;
}

// ── Synthetic population generator ───────────────────────────────────────

type SetParams = {
  nOkBuffered: number;
  nClientFault: number;
  nGatewayFault: number;
  nRetryExhausted: number;
  nStreamPreTokenFault: number; // streaming failure before first token → req_complete with stream: true
  nStreamCompleted: number;
  nStreamFaulted: number;
  nStreamClientGone: number;
  nWarmTokens: number;
  nColdTokens: number;
};

// Deterministic variation per index — every category stays ≥ 1 so no formula
// ever runs against an empty population (empty-input behavior is not part of
// the SLI contract).
function paramsFor(i: number): SetParams {
  return {
    nOkBuffered: 12 + (i % 7),
    nClientFault: 2 + (i % 5),
    nGatewayFault: 1 + (i % 4),
    nRetryExhausted: 1 + (i % 3),
    nStreamPreTokenFault: 1 + (i % 2),
    nStreamCompleted: 8 + (i % 6),
    nStreamFaulted: 1 + (i % 3),
    nStreamClientGone: 1 + (i % 4),
    nWarmTokens: 5 + (i % 5),
    nColdTokens: 2 + (i % 3),
  };
}

type ExpectedSli = {
  gatewayOverheadP99: number;
  ttftWarmP95: number;
  ttftColdP99: number;
  completionRate: number;
  totalResponseTimeNonstreamP95: number;
  requestSuccessRate: number;
};

// Rows excluded from the non-stream p95 carry a poison duration: a filter
// leak drags the quantile far outside the ±0.5% tolerance — a loud failure
// instead of a silent near-miss.
const POISON_DURATION_MS = 10_000_000;

function makeSet(i: number): { set: EventSet; expected: ExpectedSli } {
  const p = paramsFor(i);
  const totalTerminals =
    p.nOkBuffered +
    p.nClientFault +
    p.nGatewayFault +
    p.nRetryExhausted +
    p.nStreamPreTokenFault +
    p.nStreamCompleted +
    p.nStreamFaulted +
    p.nStreamClientGone;

  // Overheads are a descending permutation of {3, 6, …, 3*totalTerminals}
  // spread across BOTH terminal event types — exercising the merge and the
  // sort while keeping the p99 in closed form.
  let issued = 0;
  const nextOverhead = (): number => {
    issued += 1;
    return 3 * (totalTerminals - issued + 1);
  };

  const set: EventSet = {
    reqComplete: [
      // Successful buffered durations descend 100*n..100 — sorted ascending
      // they are 100, 200, …, 100*n, so p95 = 100 * ceil(0.95 * n).
      ...range(p.nOkBuffered).map((j) =>
        reqComplete(null, false, 100 * (p.nOkBuffered - j), nextOverhead()),
      ),
      ...range(p.nClientFault).map(() =>
        reqComplete("client-fault", false, POISON_DURATION_MS, nextOverhead()),
      ),
      ...range(p.nGatewayFault).map(() =>
        reqComplete("gateway-fault", false, POISON_DURATION_MS, nextOverhead()),
      ),
      ...range(p.nRetryExhausted).map(() =>
        reqComplete(
          "upstream-retry-exhausted",
          false,
          POISON_DURATION_MS,
          nextOverhead(),
        ),
      ),
      ...range(p.nStreamPreTokenFault).map(() =>
        reqComplete("gateway-fault", true, POISON_DURATION_MS, nextOverhead()),
      ),
    ],
    streamDone: [
      ...range(p.nStreamCompleted).map(() =>
        streamDone(null, true, nextOverhead()),
      ),
      ...range(p.nStreamFaulted).map(() =>
        streamDone("upstream-fault", false, nextOverhead()),
      ),
      ...range(p.nStreamClientGone).map(() =>
        streamDone("client-fault", false, nextOverhead()),
      ),
    ],
    streamFirstToken: [
      ...range(p.nWarmTokens).map((j) =>
        streamFirstToken(10 * (p.nWarmTokens - j), false),
      ),
      ...range(p.nColdTokens).map((j) =>
        streamFirstToken(50 * (p.nColdTokens - j), true),
      ),
    ],
  };

  const expected: ExpectedSli = {
    gatewayOverheadP99: 3 * Math.ceil(0.99 * totalTerminals),
    ttftWarmP95: 10 * Math.ceil(0.95 * p.nWarmTokens),
    ttftColdP99: 50 * Math.ceil(0.99 * p.nColdTokens),
    completionRate:
      p.nStreamCompleted / (p.nStreamCompleted + p.nStreamFaulted),
    totalResponseTimeNonstreamP95: 100 * Math.ceil(0.95 * p.nOkBuffered),
    requestSuccessRate:
      (p.nOkBuffered +
        p.nClientFault +
        p.nStreamCompleted +
        p.nStreamClientGone) /
      totalTerminals,
  };

  return { set, expected };
}

function expectWithinHalfPercent(
  actual: number,
  expected: number,
  label: string,
): void {
  const relativeError = Math.abs(actual - expected) / expected;
  expect(
    relativeError,
    `${label}: got ${actual}, expected ${expected}`,
  ).toBeLessThanOrEqual(0.005);
}

describe("SLI formulas — query coverage smoke", () => {
  it("all six formulas match closed-form expectations on 50 synthetic event sets (±0.5%)", () => {
    for (let i = 0; i < 50; i++) {
      const { set, expected } = makeSet(i);

      expectWithinHalfPercent(
        gatewayOverheadP99(set),
        expected.gatewayOverheadP99,
        `set ${i} gateway_overhead_p99`,
      );
      expectWithinHalfPercent(
        ttftWarmP95(set),
        expected.ttftWarmP95,
        `set ${i} ttft_warm_p95`,
      );
      expectWithinHalfPercent(
        ttftColdP99(set),
        expected.ttftColdP99,
        `set ${i} ttft_cold_p99`,
      );
      expectWithinHalfPercent(
        completionRate(set),
        expected.completionRate,
        `set ${i} completion_rate`,
      );
      expectWithinHalfPercent(
        totalResponseTimeNonstreamP95(set),
        expected.totalResponseTimeNonstreamP95,
        `set ${i} total_response_time_nonstream_p95`,
      );
      expectWithinHalfPercent(
        requestSuccessRate(set),
        expected.requestSuccessRate,
        `set ${i} request_success_rate_30d`,
      );
    }
  });

  it("completion_rate: 100 done, 5 gateway-fault, 10 client-gone → exactly 100/105", () => {
    const set: EventSet = {
      reqComplete: [],
      streamFirstToken: [],
      streamDone: [
        ...range(100).map(() => streamDone(null, true, 1)),
        ...range(5).map(() => streamDone("gateway-fault", false, 1)),
        ...range(10).map(() => streamDone("client-fault", false, 1)),
      ],
    };

    expect(completionRate(set)).toBeCloseTo(100 / 105, 10);
  });

  it("request_success_rate_30d: 4xx counts as success → exactly 980/1000", () => {
    const set: EventSet = {
      streamDone: [],
      streamFirstToken: [],
      reqComplete: [
        ...range(950).map(() => reqComplete(null, false, 100, 1)),
        ...range(30).map(() => reqComplete("client-fault", false, 100, 1)),
        ...range(13).map(() =>
          reqComplete("upstream-retry-exhausted", false, 100, 1),
        ),
        ...range(2).map(() => reqComplete("upstream-fault", false, 100, 1)),
        ...range(5).map(() => reqComplete("gateway-fault", false, 100, 1)),
      ],
    };

    expect(requestSuccessRate(set)).toBeCloseTo(980 / 1000, 10);
  });
});
