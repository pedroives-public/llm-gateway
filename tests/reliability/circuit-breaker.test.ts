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

function makeCircuit(log: TestLog = noopLog) {
  return createCircuitBreaker(log);
}

function makeBreaker(log: TestLog = noopLog): CircuitBreaker {
  return makeCircuit(log).breaker;
}

// A freshly constructed breaker is CLOSED and admits the next request
// normally. Fake timers and outcome/time helpers are introduced alongside
// the sliding-window and CLOSED -> OPEN behaviour that first needs them.
describe("circuit breaker — initial state", () => {
  it("starts CLOSED and admits a request as { kind: 'NORMAL' }", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;

    expect(
      circuit.probe.startAttempt(),
      "no recovery attempt may start while CLOSED: there is nothing to recover from",
    ).toBeNull();
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
    const circuit = makeCircuit();
    const breaker = circuit.breaker;

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(WINDOW_MS + 1_000); // window emptied, still inside the 60s OPEN

    expect(
      circuit.probe.startAttempt(),
      "a recovery attempt must wait the full 60s of OPEN: the failure window emptying at 30s does not shorten it",
    ).toBeNull();
    expect(breaker.getState()).toBe("OPEN");
    expect(
      breaker.tryAcquire(),
      "no tenant request passes during OPEN: the upstream is resting for the 60s",
    ).toEqual({ kind: "FAST_FAIL" });
  });

  it("transitions to HALF_OPEN and admits one probe after 60s OPEN", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(OPEN_MS + 1_000); // past the 60s cooldown

    expect(circuit.probe.startAttempt()).toBeTypeOf("function");
    expect(breaker.getState()).toBe("HALF_OPEN");
    expect(
      breaker.tryAcquire(),
      "no tenant request passes during HALF_OPEN: only the probe reaches the upstream",
    ).toEqual({ kind: "FAST_FAIL" });
  });

  it("admits only one probe — a second acquire while the probe is in flight fast-fails", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(OPEN_MS + 1_000);

    expect(circuit.probe.startAttempt()).toBeTypeOf("function");
    expect(
      circuit.probe.startAttempt(),
      "at most one probe per process: a second attempt would give one recovery two votes",
    ).toBeNull();
    expect(
      breaker.tryAcquire(),
      "no tenant request passes during HALF_OPEN: only the probe reaches the upstream",
    ).toEqual({ kind: "FAST_FAIL" });
  });

  it("a tenant request after the 60s neither starts an attempt nor moves the breaker", () => {
    const breaker = makeBreaker();

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(OPEN_MS + 1_000);

    const because =
      "only the probe moves the breaker out of OPEN: no tenant request can start a recovery attempt";
    expect(breaker.tryAcquire(), because).toEqual({ kind: "FAST_FAIL" });
    expect(breaker.getState(), because).toBe("OPEN");
  });
});

// Drive the breaker to HALF_OPEN with its single probe admitted, so each test
// below starts at the moment a probe outcome is about to be recorded.
function admitProbe(circuit: ReturnType<typeof createCircuitBreaker>) {
  recordFailures(circuit.breaker, 5); // open the breaker
  vi.advanceTimersByTime(OPEN_MS + 1_000); // wait out the 60s cooldown
  const vote = circuit.probe.startAttempt();
  expect(vote).toBeTypeOf("function");
  if (!vote) throw new Error("Expected the probe vote function");
  return vote;
}

describe("circuit breaker — HALF_OPEN probe outcomes", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("closes when the probe succeeds and admits normally again", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;
    const vote = admitProbe(circuit);

    vote("SUCCESS");

    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.tryAcquire()).toEqual({ kind: "NORMAL" });
  });

  it("re-opens for another cooldown when the probe fails", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;
    const vote = admitProbe(circuit);

    vote("FAILURE");

    expect(breaker.getState()).toBe("OPEN");
    expect(
      breaker.tryAcquire(),
      "no tenant request passes during OPEN: a failed probe starts a fresh 60s",
    ).toEqual({ kind: "FAST_FAIL" });
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    expect(
      circuit.probe.startAttempt(),
      "a failed probe must release the slot: the next attempt needs it",
    ).toBeTypeOf("function");
  });

  it("stays HALF_OPEN on an inconclusive probe and admits the next request as a fresh probe", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;
    const vote = admitProbe(circuit);

    vote("INCONCLUSIVE");

    expect(breaker.getState()).toBe("HALF_OPEN");
    expect(
      circuit.probe.startAttempt(),
      "an inconclusive probe must release the slot: a stuck slot holds HALF_OPEN until restart",
    ).toBeTypeOf("function");
    expect(
      breaker.tryAcquire(),
      "no tenant request passes during HALF_OPEN: an inconclusive probe leaves the next attempt to the probe",
    ).toEqual({ kind: "FAST_FAIL" });
  });

  it("releases the probe slot when state-change logging throws", () => {
    let shouldThrow = false;
    const circuit = makeCircuit({
      info: () => {
        if (shouldThrow) throw new Error("probe event failed");
      },
    });
    const breaker = circuit.breaker;
    const vote = admitProbe(circuit);

    shouldThrow = true;
    expect(() => vote("SUCCESS")).toThrow("probe event failed");
    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.tryAcquire()).toEqual({ kind: "NORMAL" });

    shouldThrow = false;
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS + 1_000);

    expect(
      circuit.probe.startAttempt(),
      "a logging exception must not strand the next probe",
    ).toBeTypeOf("function");
  });
});

describe("circuit breaker — tenant outcomes while HALF_OPEN", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    {
      outcome: "SUCCESS",
      expectedState: "HALF_OPEN",
      expectedAdmission: "FAST_FAIL",
      because:
        "a tenant vote during HALF_OPEN must not close the breaker: only the probe of the current attempt proves recovery",
    },
    {
      outcome: "FAILURE",
      expectedState: "HALF_OPEN",
      expectedAdmission: "FAST_FAIL",
      because:
        "a tenant vote during HALF_OPEN must not reopen the breaker: it is not the result of the probe in flight",
    },
    {
      outcome: "INCONCLUSIVE",
      expectedState: "HALF_OPEN",
      expectedAdmission: "FAST_FAIL",
      because:
        "a tenant vote during HALF_OPEN must not release the probe slot: a second probe would run beside the one in flight",
    },
  ] as const)(
    "a tenant $outcome vote during HALF_OPEN moves nothing",
    ({ outcome, expectedState, expectedAdmission, because }) => {
      const circuit = makeCircuit();
      const breaker = circuit.breaker;
      admitProbe(circuit);

      breaker.recordResult(outcome);

      expect(breaker.getState(), because).toBe(expectedState);
      expect(breaker.tryAcquire(), because).toEqual({ kind: expectedAdmission });
      expect(circuit.probe.startAttempt(), because).toBeNull();
    },
  );
});

describe("circuit breaker — tenant votes while OPEN", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    {
      outcome: "SUCCESS",
      because:
        "a tenant SUCCESS vote during OPEN moves nothing: the state stays OPEN and the 60s count from the original opening",
    },
    {
      outcome: "FAILURE",
      because:
        "a tenant FAILURE vote during OPEN moves nothing: failing tenants must not postpone recovery by restarting the 60s",
    },
    {
      outcome: "INCONCLUSIVE",
      because:
        "a tenant INCONCLUSIVE vote during OPEN moves nothing: the state stays OPEN and the 60s count from the original opening",
    },
  ] as const)(
    "keeps the original cooldown after an OPEN tenant $outcome vote",
    ({ outcome, because }) => {
      const circuit = makeCircuit();
      const breaker = circuit.breaker;
      recordFailures(breaker, 5);
      vi.advanceTimersByTime(30_000);

      breaker.recordResult(outcome);

      expect(breaker.getState(), because).toBe("OPEN");
      vi.advanceTimersByTime(31_000);
      expect(circuit.probe.startAttempt(), because).toBeTypeOf("function");
    },
  );
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
    const circuit = makeCircuit({ info });
    const breaker = circuit.breaker;
    recordFailures(breaker, 5); // CLOSED -> OPEN
    info.mockClear(); // ignore the open event; assert only the next transition
    vi.advanceTimersByTime(OPEN_MS + 1_000);

    circuit.probe.startAttempt();

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
    const circuit = makeCircuit({ info });
    const breaker = circuit.breaker;
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    const vote = circuit.probe.startAttempt(); // OPEN -> HALF_OPEN
    info.mockClear();

    if (!vote) throw new Error("Expected the probe vote function");
    vote("SUCCESS");

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
    const circuit = makeCircuit({ info });
    const breaker = circuit.breaker;
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    const vote = circuit.probe.startAttempt(); // OPEN -> HALF_OPEN
    info.mockClear();

    if (!vote) throw new Error("Expected the probe vote function");
    vote("FAILURE");

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
    const circuit = makeCircuit();
    const breaker = circuit.breaker;
    const vote = admitProbe(circuit);
    vote("SUCCESS");

    recordFailures(breaker, 4);

    expect(breaker.getState()).toBe("CLOSED");

    recordFailures(breaker, 1);

    expect(breaker.getState()).toBe("OPEN");
  });
});

describe("circuit breaker — stale probe votes", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    {
      completedOutcome: "SUCCESS",
      staleOutcome: "FAILURE",
      expectedState: "CLOSED",
      expectedAdmission: "NORMAL",
      because: "a completed probe cannot add a failure after closing",
    },
    {
      completedOutcome: "FAILURE",
      staleOutcome: "SUCCESS",
      expectedState: "OPEN",
      expectedAdmission: "FAST_FAIL",
      because: "a completed probe cannot close the breaker after reopening it",
    },
    {
      completedOutcome: "INCONCLUSIVE",
      staleOutcome: "SUCCESS",
      expectedState: "HALF_OPEN",
      expectedAdmission: "FAST_FAIL",
      because: "a stale vote cannot act when no probe is in flight",
    },
  ] as const)(
    "ignores stale $staleOutcome after $completedOutcome",
    ({
      completedOutcome,
      staleOutcome,
      expectedState,
      expectedAdmission,
      because,
    }) => {
      const circuit = makeCircuit();
      const breaker = circuit.breaker;
      const voteN = admitProbe(circuit);

      voteN(completedOutcome);

      voteN(staleOutcome);

      expect(breaker.getState(), because).toBe(expectedState);
      expect(breaker.tryAcquire(), because).toEqual({
        kind: expectedAdmission,
      });
    },
  );

  it.each([
    {
      staleOutcome: "SUCCESS",
      because: "attempt N cannot close the breaker during attempt N+1",
    },
    {
      staleOutcome: "FAILURE",
      because: "attempt N cannot reopen the breaker during attempt N+1",
    },
    {
      staleOutcome: "INCONCLUSIVE",
      because: "attempt N cannot free the probe slot of attempt N+1",
    },
  ] as const)(
    "ignores a stale $staleOutcome from attempt N during attempt N+1",
    ({ staleOutcome, because }) => {
      const circuit = makeCircuit();
      const breaker = circuit.breaker;
      const voteN = admitProbe(circuit);
      voteN("FAILURE");
      vi.advanceTimersByTime(OPEN_MS + 1_000);
      const voteN1 = circuit.probe.startAttempt();
      if (!voteN1) throw new Error("Expected the vote function of attempt N+1");

      voteN(staleOutcome);

      expect(breaker.getState(), because).toBe("HALF_OPEN");
      expect(circuit.probe.startAttempt(), because).toBeNull();

      voteN1("SUCCESS");

      expect(breaker.getState(), because).toBe("CLOSED");
    },
  );

  it("spends a vote even when its state-change logging throws", () => {
    let shouldThrow = false;
    const circuit = makeCircuit({
      info: () => {
        if (shouldThrow) throw new Error("probe event failed");
      },
    });
    const voteN = admitProbe(circuit);

    shouldThrow = true;
    expect(() => voteN("FAILURE")).toThrow("probe event failed");
    shouldThrow = false;
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    const voteN1 = circuit.probe.startAttempt();
    if (!voteN1) throw new Error("Expected the vote function of attempt N+1");

    voteN("SUCCESS");

    expect(
      circuit.breaker.getState(),
      "a vote that threw is still the attempt's vote: it cannot act during attempt N+1",
    ).toBe("HALF_OPEN");
  });

  it("does not count stale failures toward opening after the breaker closed", () => {
    const circuit = makeCircuit();
    const voteN = admitProbe(circuit);
    voteN("SUCCESS");

    for (let i = 0; i < 5; i++) {
      voteN("FAILURE");
    }

    expect(
      circuit.breaker.getState(),
      "a stale probe vote must not enter the tenant failure window",
    ).toBe("CLOSED");
  });

  it("does not restart the 60s when a stale failure arrives during OPEN", () => {
    const circuit = makeCircuit();
    const voteN = admitProbe(circuit);
    voteN("FAILURE");
    vi.advanceTimersByTime(30_000);

    voteN("FAILURE");
    vi.advanceTimersByTime(31_000);

    expect(
      circuit.probe.startAttempt(),
      "a stale probe vote must not postpone the next attempt",
    ).toBeTypeOf("function");
  });
});
