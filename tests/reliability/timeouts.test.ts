import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  armWallClockTimeout,
  armTotalDurationTimeout,
} from "../../src/reliability/timeouts.js";
import { STREAM_TOTAL_DURATION_MS } from "../../src/config.js";

describe("armWallClockTimeout", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("aborts the signal with a wall_clock_expired typed reason when the deadline fires", () => {
    const { signal } = armWallClockTimeout(30_000);

    expect(signal.aborted).toBe(false);

    vi.advanceTimersByTime(30_000);

    expect(signal.aborted).toBe(true);
    // Exact pin: any extra field on the reason is deliberate contract growth.
    expect(signal.reason).toEqual({ kind: "wall_clock_expired" });
  });

  it("never aborts once clear() runs before the deadline", () => {
    const { signal, clear } = armWallClockTimeout(30_000);

    clear();
    vi.advanceTimersByTime(30_000);

    expect(signal.aborted).toBe(false);
  });

  it("clear() is idempotent — repeated calls neither throw nor abort", () => {
    const { signal, clear } = armWallClockTimeout(30_000);

    clear();
    clear();
    vi.advanceTimersByTime(30_000);

    expect(signal.aborted).toBe(false);
  });

  it("clear() after the deadline fired is a harmless no-op (abort stands)", () => {
    const { signal, clear } = armWallClockTimeout(30_000);

    vi.advanceTimersByTime(30_000);
    clear();

    expect(signal.aborted).toBe(true);
    expect(signal.reason).toEqual({ kind: "wall_clock_expired" });
  });
});

describe("armTotalDurationTimeout", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("aborts the signal with a total_timeout typed reason when the deadline value is reached", () => {
    const deadlineAt = Date.now() + STREAM_TOTAL_DURATION_MS;

    const { signal, clear } = armTotalDurationTimeout(deadlineAt, () => false);

    expect(
      signal.aborted,
      "arming must not abort: the deadline has not been reached yet",
    ).toBe(false);

    vi.advanceTimersByTime(STREAM_TOTAL_DURATION_MS);

    expect(
      signal.aborted,
      "the firing must abort the signal once Date.now() reaches deadlineAt",
    ).toBe(true);
    // Exact pin: any extra field on the reason is deliberate contract growth.
    expect(
      signal.reason,
      "the abort reason must be exactly the typed kind total_timeout",
    ).toEqual({ kind: "total_timeout" });

    clear();
  });

  it("does not abort on a firing that runs before the deadline value, and aborts once that value is reached", () => {
    const now = Date.now();
    const deadlineAt = now + STREAM_TOTAL_DURATION_MS;

    const { signal, clear } = armTotalDurationTimeout(deadlineAt, () => false);

    // Setting the clock back fires no timer, so the timer armed above now
    // fires while Date.now() is still 5 ms short of the deadline value.
    vi.setSystemTime(now - 5);
    vi.advanceTimersByTime(STREAM_TOTAL_DURATION_MS);

    expect(
      signal.aborted,
      "a firing that runs while Date.now() < deadlineAt must not abort: the firing reads the deadline value, it does not trust the timer",
    ).toBe(false);

    vi.advanceTimersByTime(5);

    expect(
      signal.aborted,
      "the signal must still be aborted once the deadline value is reached: the remainder is waited out",
    ).toBe(true);
    expect(
      signal.reason,
      "the abort reason must be exactly the typed kind total_timeout",
    ).toEqual({ kind: "total_timeout" });

    clear();
  });

  it("does nothing when a terminal was already decided before the firing: no abort, no further timer", () => {
    let isTerminalDecided = false;
    const deadlineAt = Date.now() + STREAM_TOTAL_DURATION_MS;

    const { signal, clear } = armTotalDurationTimeout(
      deadlineAt,
      () => isTerminalDecided,
    );

    // The terminal is decided after arming and before the firing: the query
    // must be read at the firing, never copied at arming.
    isTerminalDecided = true;

    vi.advanceTimersByTime(STREAM_TOTAL_DURATION_MS);

    expect(
      signal.aborted,
      "a firing that finds a terminal already decided must not abort: the request ended for another cause",
    ).toBe(false);
    expect(
      vi.getTimerCount(),
      "a firing that finds a terminal already decided must arm no further timer",
    ).toBe(0);

    clear();
  });

  it("reads the terminal before the deadline value: an early firing with a terminal already decided arms no timer", () => {
    let isTerminalDecided = false;
    const now = Date.now();
    const deadlineAt = now + STREAM_TOTAL_DURATION_MS;

    const { signal, clear } = armTotalDurationTimeout(
      deadlineAt,
      () => isTerminalDecided,
    );

    isTerminalDecided = true;

    // The firing runs 5 ms short of the deadline value, with a terminal
    // already decided.
    vi.setSystemTime(now - 5);
    vi.advanceTimersByTime(STREAM_TOTAL_DURATION_MS);

    expect(
      signal.aborted,
      "a firing that finds a terminal already decided must not abort, whether or not the deadline value was reached",
    ).toBe(false);
    expect(
      vi.getTimerCount(),
      "the terminal is read before the deadline value: an early firing with a terminal already decided must not arm the remainder timer",
    ).toBe(0);

    clear();
  });

  it("clear() disarms the timer armed for the remainder, so the signal never aborts afterwards", () => {
    const now = Date.now();
    const deadlineAt = now + STREAM_TOTAL_DURATION_MS;

    const { signal, clear } = armTotalDurationTimeout(deadlineAt, () => false);

    // The early firing arms a timer for the remaining 5 ms; clear() must
    // reach that one.
    vi.setSystemTime(now - 5);
    vi.advanceTimersByTime(STREAM_TOTAL_DURATION_MS);

    expect(
      vi.getTimerCount(),
      "an early firing must leave exactly one timer armed: the one for the remainder",
    ).toBe(1);
    clear();
    expect(
      vi.getTimerCount(),
      "clear() must disarm the timer that is armed when it is called, not only the first one",
    ).toBe(0);

    vi.advanceTimersByTime(5);
    expect(
      signal.aborted,
      "after clear() no firing may run: reaching the deadline value must not abort the signal",
    ).toBe(false);
  });
});

describe("armTotalDurationTimeout's narrowed abort capabilities", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("the client-departure capability aborts with a client_gone typed reason while no terminal is decided", () => {
    const armed = armTotalDurationTimeout(
      Date.now() + STREAM_TOTAL_DURATION_MS,
      () => false,
    );

    armed.abortForClientGoneIfUndecided();

    expect(
      armed.signal.aborted,
      "a departure observed before any terminal is decided wakes every wait bound to the signal",
    ).toBe(true);
    expect(
      armed.signal.reason,
      "the abort reason must be exactly the typed kind client_gone",
    ).toEqual({ kind: "client_gone" });

    armed.clear();
  });

  it("the client-departure capability does nothing once a terminal is decided, read at the call and not at arming", () => {
    let isTerminalDecided = false;
    const armed = armTotalDurationTimeout(
      Date.now() + STREAM_TOTAL_DURATION_MS,
      () => isTerminalDecided,
    );

    isTerminalDecided = true;
    armed.abortForClientGoneIfUndecided();

    expect(
      armed.signal.aborted,
      "a departure observed after a decided terminal is the loser of that race: it must not become the signal's first reason",
    ).toBe(false);

    armed.clear();
  });

  it.each([
    { kind: "total_timeout" },
    { kind: "client_gone" },
    { kind: "parser_buffer_cap" },
  ] as const)("the terminal capability aborts with the $kind typed reason once a terminal is decided", ({ kind }) => {
    let isTerminalDecided = false;
    const armed = armTotalDurationTimeout(
      Date.now() + STREAM_TOTAL_DURATION_MS,
      () => isTerminalDecided,
    );

    isTerminalDecided = true;
    armed.abortForTerminalAfterDecision(kind);

    expect(
      armed.signal.aborted,
      "the terminal block aborts with its own terminal's cause after deciding it",
    ).toBe(true);
    expect(
      armed.signal.reason,
      "the abort reason must be exactly the kind the terminal block passed",
    ).toEqual({ kind });

    armed.clear();
  });

  it("the terminal capability throws a named error and leaves the signal unaborted while no terminal is decided", () => {
    const armed = armTotalDurationTimeout(
      Date.now() + STREAM_TOTAL_DURATION_MS,
      () => false,
    );

    // Caught here and asserted after the signal, so each rule fails on its own
    // sentence: an abort that went through, and a refusal that stayed silent.
    let thrown: unknown;
    try {
      armed.abortForTerminalAfterDecision("total_timeout");
    } catch (error) {
      thrown = error;
    }

    expect(
      armed.signal.aborted,
      "the terminal abort comes only after a decided terminal: a call before the decision leaves the signal as it was",
    ).toBe(false);
    expect(
      thrown instanceof Error ? thrown.name : thrown,
      "a terminal abort before the decision fails loudly: the refused call throws the named error instead of being skipped",
    ).toBe("TerminalAbortBeforeDecisionError");

    armed.clear();
  });
});
