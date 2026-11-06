import {
  describe,
  it,
  expect,
  expectTypeOf,
  vi,
  beforeEach,
  afterEach,
} from "vitest";
import {
  createCircuitBreaker,
  type CircuitBreaker,
  type CircuitState,
  type ProbeVote,
} from "../../src/reliability/circuit-breaker.js";
import { PROBE_BACKSTOP_MS } from "../../src/config.js";

// The factory takes a logger ({ info, error }) so the breaker can emit
// cb_state_change on transitions — the same logger shape classify.ts uses.
// Most tests don't care about logging, so makeBreaker defaults to a no-op;
// the event tests pass a vi.fn() spy to inspect the emitted payload.
type TestLog = { info: (obj: object) => void; error: (obj: object) => void };
const noopLog: TestLog = { info: () => {}, error: () => {} };

// Every recovery attempt reaches this runner, so a test drives the probe by
// advancing the clock and reads each attempt's vote from the recorded calls.
// Tests that inspect logging pass only the side they read; the other side
// defaults to a no-op.
type PartialTestLog = Pick<TestLog, "info"> & Partial<TestLog>;

function makeCircuit(log: PartialTestLog = noopLog) {
  const runProbe = vi.fn<(vote: ProbeVote) => void>();
  return {
    breaker: createCircuitBreaker({ error: () => {}, ...log }, runProbe),
    runProbe,
  };
}

function makeBreaker(log: PartialTestLog = noopLog): CircuitBreaker {
  return makeCircuit(log).breaker;
}

function latestVote(circuit: ReturnType<typeof makeCircuit>): ProbeVote {
  const vote = circuit.runProbe.mock.lastCall?.[0];
  if (!vote) throw new Error("Expected a probe vote: no attempt has started");
  return vote;
}

// A freshly constructed breaker is CLOSED and admits the next request
// normally. Fake timers and outcome/time helpers are introduced alongside
// the sliding-window and CLOSED -> OPEN behaviour that first needs them.
describe("circuit breaker — initial state", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts CLOSED and admits a request as { kind: 'NORMAL' }", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;

    vi.advanceTimersByTime(OPEN_MS * 2);

    expect(
      circuit.runProbe,
      "no recovery attempt may start while CLOSED: there is nothing to recover from",
    ).not.toHaveBeenCalled();
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

describe("circuit breaker — scheduled recovery probe", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts the probe at the 60s boundary", () => {
    let stateAtProbeCall: CircuitState | undefined;
    const runProbe = vi.fn(() => {
      stateAtProbeCall = breaker.getState();
    });
    const breaker = createCircuitBreaker(noopLog, runProbe);

    recordFailures(breaker, 5);

    vi.advanceTimersByTime(OPEN_MS - 1);
    expect(
      runProbe,
      "the probe must wait the full 60s of OPEN",
    ).not.toHaveBeenCalled();
    expect(breaker.getState()).toBe("OPEN");

    vi.advanceTimersByTime(1);
    expect(
      runProbe,
      "the timer must start the probe at 60s, with no tenant request",
    ).toHaveBeenCalledTimes(1);
    expect(
      runProbe,
      "the probe must receive the vote of its attempt and the signal that aborts it",
    ).toHaveBeenCalledWith(expect.any(Function), expect.any(AbortSignal));
    expect(
      stateAtProbeCall,
      "the transition to HALF_OPEN must happen before the probe runs",
    ).toBe("HALF_OPEN");
    expect(breaker.getState()).toBe("HALF_OPEN");
  });

  it("arms the probe even when logging the opening throws", () => {
    let shouldThrow = true;
    const runProbe = vi.fn();
    const breaker = createCircuitBreaker(
      {
        info: () => {
          if (shouldThrow) throw new Error("open event failed");
        },
        error: () => {},
      },
      runProbe,
    );

    expect(() => recordFailures(breaker, 5)).toThrow(
      "open event failed",
    );
    shouldThrow = false;
    vi.advanceTimersByTime(OPEN_MS);

    expect(
      runProbe,
      "a logging exception must not leave OPEN without a timer: nothing else leaves OPEN",
    ).toHaveBeenCalledTimes(1);
  });

  it("does not keep the process alive while waiting for the probe", () => {
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
    const { breaker } = makeCircuit();

    recordFailures(breaker, 5);

    expect(setTimeoutSpy).toHaveBeenCalledTimes(1);
    expect(
      setTimeoutSpy.mock.results[0]?.value.hasRef(),
      "the probe timer must not keep the process alive",
    ).toBe(false);
    setTimeoutSpy.mockRestore();
  });

  it("starts the probe on time when the wall clock moves back", () => {
    const { breaker, runProbe } = makeCircuit();

    recordFailures(breaker, 5);
    vi.setSystemTime(Date.now() - 1);
    vi.advanceTimersByTime(OPEN_MS);

    expect(
      runProbe,
      "the timer alone decides the 60s: a wall-clock check must not refuse its firing",
    ).toHaveBeenCalledTimes(1);
    expect(breaker.getState()).toBe("HALF_OPEN");
  });

  it("rearms the probe timer after a failed probe vote", () => {
    const runProbe = vi.fn<(vote: ProbeVote) => void>();
    const breaker = createCircuitBreaker(noopLog, runProbe);

    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS);

    expect(runProbe).toHaveBeenCalledTimes(1);
    const firstVote = runProbe.mock.calls[0]?.[0];
    expect(firstVote).toBeTypeOf("function");
    if (!firstVote) throw new Error("Expected the first probe vote");

    firstVote("FAILURE");

    vi.advanceTimersByTime(OPEN_MS - 1);
    expect(
      runProbe,
      "a failed probe must wait a fresh 60s before the next attempt",
    ).toHaveBeenCalledTimes(1);
    expect(breaker.getState()).toBe("OPEN");

    vi.advanceTimersByTime(1);
    expect(
      runProbe,
      "a failed probe must schedule the next attempt, with no tenant request",
    ).toHaveBeenCalledTimes(2);
    expect(runProbe).toHaveBeenNthCalledWith(
      2,
      expect.any(Function),
      expect.any(AbortSignal),
    );
    expect(breaker.getState()).toBe("HALF_OPEN");
  });

  it("rearms the probe timer even when logging the reopening throws", () => {
    let shouldThrow = false;
    let thrown = 0;
    const runProbe = vi.fn<(vote: ProbeVote) => void>();
    const breaker = createCircuitBreaker(
      {
        info: () => {
          if (shouldThrow) {
            thrown += 1;
            throw new Error("reopen event failed");
          }
        },
        error: () => {},
      },
      runProbe,
    );

    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS);
    const firstVote = runProbe.mock.calls[0]?.[0];
    if (!firstVote) throw new Error("Expected the first probe vote");

    shouldThrow = true;
    expect(
      () => firstVote("FAILURE"),
      "a probe vote never throws to its caller",
    ).not.toThrow();
    expect(
      thrown,
      "both breaker lines of the vote were attempted, and both threw",
    ).toBe(2);
    shouldThrow = false;
    vi.advanceTimersByTime(OPEN_MS);

    expect(
      runProbe,
      "a logging exception must not leave the reopened breaker without a timer: nothing else leaves OPEN",
    ).toHaveBeenCalledTimes(2);
  });
});

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
      circuit.runProbe,
      "a recovery attempt must wait the full 60s of OPEN: the failure window emptying at 30s does not shorten it",
    ).not.toHaveBeenCalled();
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

    expect(circuit.runProbe).toHaveBeenCalledTimes(1);
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
    // The attempt stays in flight until just before its backstop.
    vi.advanceTimersByTime(OPEN_MS + PROBE_BACKSTOP_MS - 1);

    expect(
      circuit.runProbe,
      "at most one probe per process: a second attempt would give one recovery two votes",
    ).toHaveBeenCalledTimes(1);
    expect(
      breaker.tryAcquire(),
      "no tenant request passes during HALF_OPEN: only the probe reaches the upstream",
    ).toEqual({ kind: "FAST_FAIL" });
  });

  it("a tenant request during OPEN neither starts an attempt nor moves the breaker", () => {
    const circuit = makeCircuit();
    const breaker = circuit.breaker;

    recordFailures(breaker, 5); // opens
    vi.advanceTimersByTime(OPEN_MS - 1);

    const because =
      "only the timer moves the breaker out of OPEN: no tenant request can start a recovery attempt";
    expect(breaker.tryAcquire(), because).toEqual({ kind: "FAST_FAIL" });
    expect(breaker.getState(), because).toBe("OPEN");
    expect(circuit.runProbe, because).not.toHaveBeenCalled();
  });
});

// Drive the breaker to HALF_OPEN with its single probe admitted, so each test
// below starts at the moment a probe outcome is about to be recorded.
function admitProbe(circuit: ReturnType<typeof makeCircuit>): ProbeVote {
  recordFailures(circuit.breaker, 5); // open the breaker
  vi.advanceTimersByTime(OPEN_MS); // the timer starts the attempt
  return latestVote(circuit);
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
    vi.advanceTimersByTime(OPEN_MS);
    expect(
      circuit.runProbe,
      "a failed probe must schedule the next attempt",
    ).toHaveBeenCalledTimes(2);
  });

  it("a probe vote accepts only SUCCESS or FAILURE: every probe exit needs a next state", () => {
    expectTypeOf<Parameters<ProbeVote>[0]>().toEqualTypeOf<
      "SUCCESS" | "FAILURE"
    >();
  });

  it("reaches the next attempt after logging the close throws", () => {
    let shouldThrow = false;
    let thrown = 0;
    const circuit = makeCircuit({
      info: () => {
        if (shouldThrow) {
          thrown += 1;
          throw new Error("probe event failed");
        }
      },
    });
    const breaker = circuit.breaker;
    const vote = admitProbe(circuit);

    shouldThrow = true;
    expect(
      () => vote("SUCCESS"),
      "a probe vote never throws to its caller",
    ).not.toThrow();
    expect(
      thrown,
      "both breaker lines of the vote were attempted, and both threw",
    ).toBe(2);
    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.tryAcquire()).toEqual({ kind: "NORMAL" });

    shouldThrow = false;
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS);

    expect(
      circuit.runProbe,
      "a logging exception must not strand the next probe",
    ).toHaveBeenCalledTimes(2);
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
      vi.advanceTimersByTime(OPEN_MS);
      expect(circuit.runProbe, because).toHaveBeenCalledTimes(1);
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
      vi.advanceTimersByTime(30_000 - 1);
      expect(circuit.runProbe, because).not.toHaveBeenCalled();
      vi.advanceTimersByTime(1);
      expect(circuit.runProbe, because).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(OPEN_MS);
      expect(circuit.runProbe, because).toHaveBeenCalledTimes(1);
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
    const vote = latestVote(circuit); // OPEN -> HALF_OPEN
    info.mockClear();

    vote("SUCCESS");

    const stateChanges = info.mock.calls
      .map(([line]) => line)
      .filter(
        (line) =>
          (line as { event?: string }).event === "cb_state_change",
      );
    expect(stateChanges).toEqual([{
      event: "cb_state_change",
      from: "HALF_OPEN",
      to: "CLOSED",
      failure_count: 0,
      window_start_ms: 0,
    }]);
  });

  it("emits HALF_OPEN -> OPEN with an empty-window snapshot when the probe fails", () => {
    const info = vi.fn();
    const circuit = makeCircuit({ info });
    const breaker = circuit.breaker;
    recordFailures(breaker, 5);
    vi.advanceTimersByTime(OPEN_MS + 1_000);
    const vote = latestVote(circuit); // OPEN -> HALF_OPEN
    info.mockClear();

    vote("FAILURE");

    const stateChanges = info.mock.calls
      .map(([line]) => line)
      .filter(
        (line) =>
          (line as { event?: string }).event === "cb_state_change",
      );
    expect(stateChanges).toEqual([{
      event: "cb_state_change",
      from: "HALF_OPEN",
      to: "OPEN",
      failure_count: 0,
      window_start_ms: 0,
    }]);
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
  ] as const)(
    "ignores a stale $staleOutcome from attempt N during attempt N+1",
    ({ staleOutcome, because }) => {
      const circuit = makeCircuit();
      const breaker = circuit.breaker;
      const voteN = admitProbe(circuit);
      voteN("FAILURE");
      vi.advanceTimersByTime(OPEN_MS);
      const voteN1 = latestVote(circuit);

      voteN(staleOutcome);

      expect(breaker.getState(), because).toBe("HALF_OPEN");
      // Attempt N+1 stays in flight until just before its backstop.
      vi.advanceTimersByTime(PROBE_BACKSTOP_MS - 1);
      expect(
        circuit.runProbe,
        "attempt N cannot start a third attempt beside attempt N+1",
      ).toHaveBeenCalledTimes(2);

      voteN1("SUCCESS");

      expect(breaker.getState(), because).toBe("CLOSED");
    },
  );

  it("spends a vote even when its state-change logging throws", () => {
    let shouldThrow = false;
    let thrown = 0;
    const circuit = makeCircuit({
      info: () => {
        if (shouldThrow) {
          thrown += 1;
          throw new Error("probe event failed");
        }
      },
    });
    const voteN = admitProbe(circuit);

    shouldThrow = true;
    expect(
      () => voteN("FAILURE"),
      "a probe vote never throws to its caller",
    ).not.toThrow();
    expect(
      thrown,
      "both breaker lines of the vote were attempted, and both threw",
    ).toBe(2);
    shouldThrow = false;
    vi.advanceTimersByTime(OPEN_MS);

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

  it("keeps a single on-time attempt when a stale failure arrives during OPEN", () => {
    const circuit = makeCircuit();
    const voteN = admitProbe(circuit);
    voteN("FAILURE");
    vi.advanceTimersByTime(30_000);

    voteN("FAILURE");
    vi.advanceTimersByTime(30_000);

    expect(
      circuit.runProbe,
      "a stale probe vote must not postpone the next attempt",
    ).toHaveBeenCalledTimes(2);

    vi.advanceTimersByTime(30_000);
    expect(
      circuit.runProbe,
      "a stale probe vote must not add an attempt",
    ).toHaveBeenCalledTimes(2);
  });
});

// Closing the app stops the breaker permanently: a close is not evidence
// about the upstream, so the aborted attempt leaves no vote, no timer and no
// event.
describe("circuit breaker — after close", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts no probe after a close while the timer waits", () => {
    const runProbe = vi.fn<(vote: ProbeVote, signal: AbortSignal) => void>();
    const breaker = createCircuitBreaker(noopLog, runProbe);
    recordFailures(breaker, 5);

    breaker.close();
    expect(
      vi.getTimerCount(),
      "the close clears the pending timer",
    ).toBe(0);
    vi.advanceTimersByTime(OPEN_MS);

    expect(
      runProbe.mock.calls.length,
      "a closed app must not start a probe",
    ).toBe(0);
  });

  // A close lets the requests in flight finish, and they still vote.
  it("starts no probe from tenant failures that arrive after a close", () => {
    const runProbe = vi.fn<(vote: ProbeVote, signal: AbortSignal) => void>();
    const breaker = createCircuitBreaker(noopLog, runProbe);

    breaker.close();
    recordFailures(breaker, 5);
    expect(vi.getTimerCount(), "a closed app arms no timer").toBe(0);
    vi.advanceTimersByTime(OPEN_MS);

    expect(
      runProbe.mock.calls.length,
      "a closed app must not start a probe",
    ).toBe(0);
  });

  it.each(["SUCCESS", "FAILURE"] as const)(
    "aborts the probe in flight and ignores its late %s vote",
    (late) => {
      const info = vi.fn<(obj: object) => void>();
      const error = vi.fn<(obj: object) => void>();
      const runProbe = vi.fn<(vote: ProbeVote, signal: AbortSignal) => void>();
      const breaker = createCircuitBreaker({ info, error }, runProbe);
      recordFailures(breaker, 5);
      vi.advanceTimersByTime(OPEN_MS);
      const [vote, signal] = runProbe.mock.calls[0] ?? [];
      if (!vote || !signal) throw new Error("Expected a probe in flight");
      info.mockClear();

      breaker.close();
      expect(
        vi.getTimerCount(),
        "closing the app cancels the backstop of the probe in flight: no breaker timer stays pending",
      ).toBe(0);
      expect(
        () => vote(late),
        "a late vote never throws to its caller",
      ).not.toThrow();
      vi.advanceTimersByTime(OPEN_MS);

      expect(
        signal.aborted,
        "closing the app aborts the probe in flight",
      ).toBe(true);
      expect(
        breaker.getState(),
        "a vote that arrives after the close moves nothing",
      ).toBe("HALF_OPEN");
      expect(
        info.mock.calls,
        "a vote that arrives after the close writes no state change and no probe event",
      ).toEqual([]);
      expect(
        runProbe.mock.calls.length,
        "a closed app must not start a probe",
      ).toBe(1);
      expect(
        error.mock.calls,
        "a closed breaker writes no defect line for the attempt it aborted",
      ).toEqual([]);
    },
  );
});

describe("circuit breaker — the backstop bounds every probe attempt", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not keep the process alive while a probe attempt runs", async () => {
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
    const runProbe = vi.fn(() => new Promise<void>(() => {}));
    const breaker = createCircuitBreaker(
      { info: () => {}, error: () => {} },
      runProbe,
    );

    recordFailures(breaker, 5);
    await vi.advanceTimersByTimeAsync(OPEN_MS);

    const backstopCall = setTimeoutSpy.mock.calls.findIndex(
      ([, delay]) => delay === PROBE_BACKSTOP_MS,
    );
    expect(backstopCall, "the attempt arms its backstop").toBeGreaterThanOrEqual(0);
    expect(
      setTimeoutSpy.mock.results[backstopCall]?.value.hasRef(),
      "the backstop timer must not keep the process alive",
    ).toBe(false);
    setTimeoutSpy.mockRestore();
  });

  it("votes FAILURE for an attempt that never votes", async () => {
    const infoLines: object[] = [];
    const errorLines: object[] = [];
    const log = {
      info: (line: object): void => {
        infoLines.push(line);
      },
      error: (line: object): void => {
        errorLines.push(line);
      },
    };
    const votes: ProbeVote[] = [];
    const signals: AbortSignal[] = [];
    // A runner that hangs: it never votes and its promise never settles.
    const runProbe = vi.fn((vote: ProbeVote, signal: AbortSignal) => {
      votes.push(vote);
      signals.push(signal);
      return new Promise<void>(() => {});
    });
    const breaker = createCircuitBreaker(log, runProbe);

    const openedAt = Date.now();
    recordFailures(breaker, 5);
    await vi.advanceTimersByTimeAsync(OPEN_MS);

    expect(
      runProbe,
      "the scene starts exactly one attempt",
    ).toHaveBeenCalledTimes(1);
    expect(
      votes,
      "the attempt received its vote",
    ).toHaveLength(1);
    expect(
      signals,
      "the attempt received its signal",
    ).toHaveLength(1);
    expect(
      breaker.getState(),
      "the attempt is in flight",
    ).toBe("HALF_OPEN");

    const beforeBackstopInfo = [
      {
        event: "cb_state_change",
        from: "CLOSED",
        to: "OPEN",
        failure_count: 5,
        window_start_ms: openedAt,
      },
      {
        event: "cb_state_change",
        from: "OPEN",
        to: "HALF_OPEN",
        failure_count: 0,
        window_start_ms: 0,
      }
    ];

    await vi.advanceTimersByTimeAsync(PROBE_BACKSTOP_MS - 1);

    expect(
      breaker.getState(),
      "before its backstop, an attempt that has not voted stays in flight",
    ).toBe("HALF_OPEN");
    expect(
      infoLines,
      "before its backstop, nothing is decided for the attempt",
    ).toEqual(beforeBackstopInfo);
    expect(
      errorLines,
      "the defect line appears only when the backstop fires",
    ).toEqual([]);
    expect(
      signals[0]?.aborted,
      "the attempt is not aborted before its backstop",
    ).toBe(false);

    await vi.advanceTimersByTimeAsync(1);

    expect(
      breaker.getState(),
      "at its backstop, an attempt that never voted counts as FAILURE and the breaker reopens",
    ).toBe("OPEN");
    expect(
      infoLines,
      "the backstop's FAILURE goes through the same vote: one state change, one probe result, nothing else",
    ).toEqual([
      ...beforeBackstopInfo,
      {
        event: "cb_state_change",
        from: "HALF_OPEN",
        to: "OPEN",
        failure_count: 0,
        window_start_ms: 0,
      },
      {
        event: "breaker_probe_result",
        attempt: 1,
        outcome: "FAILURE",
      },
    ]);

    expect(
      errorLines,
      "the backstop writes one defect line, at error level, naming the attempt",
    ).toEqual([
      {
        event: "breaker_probe_backstop",
        attempt: 1,
      },
    ]);
    expect(
      signals[0]?.aborted,
      "the backstop aborts the attempt it ended",
    ).toBe(true);

    await vi.advanceTimersByTimeAsync(OPEN_MS);

    expect(
      breaker.getState(),
      "the next attempt starts one cooldown after the backstop reopened the breaker",
    ).toBe("HALF_OPEN");
    expect(
      runProbe,
      "exactly one new attempt after the cooldown",
    ).toHaveBeenCalledTimes(2);
    expect(
      votes,
      "the new attempt received its vote",
    ).toHaveLength(2);

    const infoBeforeLateVote = [...infoLines];
    const errorsBeforeLateVote = [...errorLines];

    votes[0]?.("SUCCESS");

    expect(
      breaker.getState(),
      "a late vote from the attempt the backstop ended changes nothing",
    ).toBe("HALF_OPEN");
    expect(
      runProbe,
      "a late vote from the attempt the backstop ended changes nothing",
    ).toHaveBeenCalledTimes(2);
    expect(
      infoLines,
      "a late vote from the attempt the backstop ended changes nothing",
    ).toEqual(infoBeforeLateVote);
    expect(
      errorLines,
      "a late vote from the attempt the backstop ended changes nothing",
    ).toEqual(errorsBeforeLateVote);
  });

  it.each([["SUCCESS", "CLOSED"], ["FAILURE", "OPEN"]] as const)(
    "does not fire the backstop after an on-time %s vote",
    async (outcome, expectedState) => {
      const infoLines: object[] = [];
      const errorLines: object[] = [];

      const log = {
        info: (line: object): void => {
          infoLines.push(line);
        },
        error: (line: object): void => {
          errorLines.push(line);
        },
      };

      const votes: ProbeVote[] = [];
      const signals: AbortSignal[] = [];
      const runProbe = vi.fn((vote: ProbeVote, signal: AbortSignal) => {
        votes.push(vote);
        signals.push(signal);
        return new Promise<void>(() => {});
      });
      const breaker = createCircuitBreaker(log, runProbe);

      recordFailures(breaker, 5);
      await vi.advanceTimersByTimeAsync(OPEN_MS);

      expect(
        runProbe,
        "the scene starts exactly one attempt",
      ).toHaveBeenCalledTimes(1);
      expect(
        votes,
        "the attempt received its vote",
      ).toHaveLength(1);
      expect(
        signals,
        "the attempt received its signal",
      ).toHaveLength(1);
      expect(
        breaker.getState(),
        "the attempt is in flight",
      ).toBe("HALF_OPEN");

      const earlyVoteMs = 5_000;
      await vi.advanceTimersByTimeAsync(earlyVoteMs);
      votes[0]?.(outcome);

      expect(
        breaker.getState(),
        "an on-time vote decides the attempt",
      ).toBe(expectedState);

      await vi.advanceTimersByTimeAsync(
        PROBE_BACKSTOP_MS - earlyVoteMs + 1,
      );

      expect(
        breaker.getState(),
        "the backstop of an attempt that already voted does nothing",
      ).toBe(expectedState);
      expect(
        runProbe,
        "the backstop of a voted attempt starts no attempt",
      ).toHaveBeenCalledTimes(1);
      expect(
        signals[0]?.aborted,
        "the backstop of a voted attempt does not abort it",
      ).toBe(false);
      expect(
        errorLines,
        "an attempt that voted on time never produces the defect line",
      ).toEqual([]);

      const probeResults = infoLines.filter(
        (line) =>
          (line as { event?: string }).event === "breaker_probe_result",
      );
      expect(
        probeResults,
        "the on-time vote is the only probe result",
      ).toEqual([
        {
          event: "breaker_probe_result",
          attempt: 1,
          outcome,
        },
      ]);

      const backstopInfoLines = infoLines.filter(
        (line) =>
          (line as { event?: string }).event === "breaker_probe_backstop",
      );
      expect(
        backstopInfoLines,
        "no backstop line at any level after an on-time vote",
      ).toEqual([]);
    },
  );
});
