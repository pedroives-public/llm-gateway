import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  createCircuitBreaker,
  type CircuitBreaker,
} from "../../src/reliability/circuit-breaker.js";

// The factory takes a logger ({ info }) so the breaker can emit
// cb_state_change on transitions — the same logger shape classify.ts uses.
// Most tests don't care about logging, so makeBreaker defaults to a no-op;
// the event tests pass a vi.fn() spy to inspect the emitted payload.
type TestLog = { info: (obj: object) => void };
const noopLog: TestLog = { info: () => {} };

function makeBreaker(log: TestLog = noopLog): CircuitBreaker {
  return createCircuitBreaker(log);
}

// A freshly constructed breaker is CLOSED and admits the next request
// normally. Fake timers and outcome/time helpers are introduced alongside
// the sliding-window and CLOSED -> OPEN behaviour that first needs them.
describe("circuit breaker — initial state", () => {
  it("starts CLOSED and admits a request as { kind: 'NORMAL' }", () => {
    const breaker = makeBreaker();

    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.tryAcquire()).toEqual({ kind: "NORMAL" });
  });
});

const WINDOW_MS = 30_000;

// Feed `count` breaker-countable failures back-to-back at the current
// (fake) clock instant.
function recordFailures(breaker: CircuitBreaker, count: number): void {
  for (let i = 0; i < count; i++) {
    breaker.recordResult("FAILURE");
  }
}

describe("circuit breaker — CLOSED to OPEN on sustained failure", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("stays CLOSED after 4 failures within the window", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 4);

    expect(breaker.getState()).toBe("CLOSED");
  });

  it("opens after the 5th failure within the window", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 5);

    expect(breaker.getState()).toBe("OPEN");
  });

  it("stays CLOSED when the 5th failure falls outside the 30s window", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 4); // stamped at the window's origin instant
    vi.advanceTimersByTime(WINDOW_MS + 1_000); // the four age past 30s
    recordFailures(breaker, 1); // lone failure inside the live window

    expect(breaker.getState()).toBe("CLOSED");
  });

  it("does not count SUCCESS or INCONCLUSIVE toward opening", () => {
    const breaker = makeBreaker();

    // Six non-failure outcomes — past the limit of five — must not open:
    // only a breaker-countable FAILURE advances the window.
    breaker.recordResult("SUCCESS");
    breaker.recordResult("SUCCESS");
    breaker.recordResult("INCONCLUSIVE");
    breaker.recordResult("SUCCESS");
    breaker.recordResult("INCONCLUSIVE");
    breaker.recordResult("SUCCESS");

    expect(breaker.getState()).toBe("CLOSED");
  });
});

const OPEN_MS = 60_000;

describe("circuit breaker — OPEN and HALF_OPEN transitions", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("fast-fails while OPEN, even after the 30s failure window has elapsed", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(WINDOW_MS + 1_000); // window emptied, still inside the 60s OPEN

    expect(breaker.getState()).toBe("OPEN");
    expect(breaker.tryAcquire()).toEqual({ kind: "FAST_FAIL" });
  });

  it("transitions to HALF_OPEN and admits one probe after 60s OPEN", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(OPEN_MS + 1_000); // past the 60s cooldown

    expect(breaker.tryAcquire()).toEqual({ kind: "PROBE" });
    expect(breaker.getState()).toBe("HALF_OPEN");
  });

  it("admits only one probe — a second acquire while the probe is in flight fast-fails", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(OPEN_MS + 1_000);

    expect(breaker.tryAcquire()).toEqual({ kind: "PROBE" }); // the single probe
    expect(breaker.tryAcquire()).toEqual({ kind: "FAST_FAIL" }); // no second probe
  });
});

// Drive the breaker to HALF_OPEN with its single probe admitted, so each test
// below starts at the moment a probe outcome is about to be recorded.
function admitProbe(breaker: CircuitBreaker): void {
  recordFailures(breaker, 5); // open the breaker
  vi.advanceTimersByTime(OPEN_MS + 1_000); // wait out the 60s cooldown
  expect(breaker.tryAcquire()).toEqual({ kind: "PROBE" }); // admit the one probe
}

describe("circuit breaker — HALF_OPEN probe outcomes", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("closes when the probe succeeds and admits normally again", () => {
    const breaker = makeBreaker();
    admitProbe(breaker);

    breaker.recordResult("SUCCESS");

    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.tryAcquire()).toEqual({ kind: "NORMAL" });
  });

  it("re-opens for another cooldown when the probe fails", () => {
    const breaker = makeBreaker();
    admitProbe(breaker);

    breaker.recordResult("FAILURE");

    expect(breaker.getState()).toBe("OPEN");
    expect(breaker.tryAcquire()).toEqual({ kind: "FAST_FAIL" }); // inside the fresh 60s
  });

  it("stays HALF_OPEN on an inconclusive probe and admits the next request as a fresh probe", () => {
    const breaker = makeBreaker();
    admitProbe(breaker);

    breaker.recordResult("INCONCLUSIVE");

    expect(breaker.getState()).toBe("HALF_OPEN");
    expect(breaker.tryAcquire()).toEqual({ kind: "PROBE" }); // slot released — no wedge
  });
});

// Each transition emits cb_state_change. failure_count / window_start_ms are a
// LIVE-window snapshot at transition time: real values on CLOSED -> OPEN, and
// 0 / 0 on the recovery transitions (they fire 60s+ later, window empty).
describe("circuit breaker — cb_state_change events", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("emits CLOSED -> OPEN with the live-window snapshot", () => {
    const info = vi.fn();
    const breaker = makeBreaker({ info });
    const windowStart = Date.now();

    recordFailures(breaker, 5);

    expect(info).toHaveBeenCalledTimes(1);
    expect(info).toHaveBeenCalledWith({
      event: "cb_state_change",
      from: "CLOSED",
      to: "OPEN",
      failure_count: 5,
      window_start_ms: windowStart,
    });
  });

  it("emits OPEN -> HALF_OPEN with an empty-window snapshot", () => {
    const info = vi.fn();
    const breaker = makeBreaker({ info });
    recordFailures(breaker, 5); // CLOSED -> OPEN
    info.mockClear(); // ignore the open event; assert only the next transition
    vi.advanceTimersByTime(OPEN_MS + 1_000);

    breaker.tryAcquire();

    expect(info).toHaveBeenCalledTimes(1);
    expect(info).toHaveBeenCalledWith({
      event: "cb_state_change",
      from: "OPEN",
      to: "HALF_OPEN",
      failure_count: 0,
      window_start_ms: 0,
    });
  });

  it("emits HALF_OPEN -> CLOSED with an empty-window snapshot when the probe succeeds", () => {
    const info = vi.fn();
    const breaker = makeBreaker({ info });
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    breaker.tryAcquire(); // OPEN -> HALF_OPEN
    info.mockClear();

    breaker.recordResult("SUCCESS");

    expect(info).toHaveBeenCalledTimes(1);
    expect(info).toHaveBeenCalledWith({
      event: "cb_state_change",
      from: "HALF_OPEN",
      to: "CLOSED",
      failure_count: 0,
      window_start_ms: 0,
    });
  });

  it("emits HALF_OPEN -> OPEN with an empty-window snapshot when the probe fails", () => {
    const info = vi.fn();
    const breaker = makeBreaker({ info });
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    breaker.tryAcquire(); // OPEN -> HALF_OPEN
    info.mockClear();

    breaker.recordResult("FAILURE");

    expect(info).toHaveBeenCalledTimes(1);
    expect(info).toHaveBeenCalledWith({
      event: "cb_state_change",
      from: "HALF_OPEN",
      to: "OPEN",
      failure_count: 0,
      window_start_ms: 0,
    });
  });
});

// After recovery (OPEN -> HALF_OPEN -> CLOSED), reopening must need a full 5
// FRESH failures — nothing recorded mid-cycle may pre-load the window. The
// burst must land with NO time advance after the close, else time-expiry hides
// a leak and the test passes even when broken (pure expiry is covered above).
describe("circuit breaker — failure-window isolation across recovery", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("requires a full 5 fresh failures to re-open after a probe-success close", () => {
    const breaker = makeBreaker();
    admitProbe(breaker);
    breaker.recordResult("SUCCESS");

    recordFailures(breaker, 4);

    expect(breaker.getState()).toBe("CLOSED");

    recordFailures(breaker, 1);

    expect(breaker.getState()).toBe("OPEN");
  });
});
