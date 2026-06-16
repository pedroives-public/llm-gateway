import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { retry, backoffCeiling } from "../../src/reliability/retry.js";
import type { Outcome } from "../../src/upstream/outcome.js";

describe("retry", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("retries once and returns the second outcome when attempt 1 is retry-eligible and attempt 2 succeeds", async () => {
    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "service unavailable",
    };
    const attempt2: Outcome = {
      kind: "ok",
      status: 200,
      body_parsed: { ok: true },
    };

    const op = vi
      .fn<() => Promise<Outcome>>()
      .mockResolvedValueOnce(attempt1)
      .mockResolvedValueOnce(attempt2);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    // Drive the backoff sleep between the two attempts to completion.
    await vi.runAllTimersAsync();

    await expect(result).resolves.toEqual(attempt2);
    expect(op).toHaveBeenCalledTimes(2);
  });

  it("waits the full Retry-After before the second attempt when the first outcome carries one", async () => {
    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 429,
      body_raw: "rate limited",
      retry_after: "3", // seconds
    };
    const attempt2: Outcome = {
      kind: "ok",
      status: 200,
      body_parsed: { ok: true },
    };

    const op = vi
      .fn<() => Promise<Outcome>>()
      .mockResolvedValueOnce(attempt1)
      .mockResolvedValueOnce(attempt2);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    // Just before the Retry-After elapses, the second attempt must NOT have fired.
    await vi.advanceTimersByTimeAsync(2_999);
    expect(op).toHaveBeenCalledTimes(1);

    // Crossing the 3s mark releases the retry.
    await vi.advanceTimersByTimeAsync(1);
    expect(op).toHaveBeenCalledTimes(2);

    await expect(result).resolves.toEqual(attempt2);
  });

  it("skips the retry without sleeping when the Retry-After exceeds the remaining budget", async () => {
    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
      retry_after: "5", // 5s wait
    };
    const attempt2: Outcome = {
      kind: "ok",
      status: 200,
      body_parsed: { ok: true },
    };

    const op = vi
      .fn<() => Promise<Outcome>>()
      .mockResolvedValueOnce(attempt1)
      .mockResolvedValueOnce(attempt2);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 2_000, // only 2s of budget; the 5s Retry-After cannot fit
      firstByteFlushed: () => false,
    });

    // Let attempt 1 resolve and the skip decision run, without advancing the clock.
    await vi.advanceTimersByTimeAsync(0);

    // Budget skip: no sleep timer scheduled, no second attempt issued...
    expect(vi.getTimerCount()).toBe(0);
    expect(op).toHaveBeenCalledTimes(1);
    // ...and attempt 1's outcome is returned verbatim (passthrough, not a 504/aborted).
    await expect(result).resolves.toEqual(attempt1);
  });

  it("does not retry a retry-eligible outcome when the signal is already aborted", async () => {
    const attempt1: Outcome = { kind: "network_failed", pre_send_proven: true };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const controller = new AbortController();
    controller.abort({ kind: "wall_clock_expired" });

    const now = Date.now();
    const result = retry(op, {
      signal: controller.signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    await vi.advanceTimersByTimeAsync(0);

    expect(vi.getTimerCount()).toBe(0); // no backoff scheduled on a dead envelope
    expect(op).toHaveBeenCalledTimes(1);
    await expect(result).resolves.toEqual(attempt1);
  });

  it("does not retry when the first byte has already been flushed, even for a retry-eligible outcome", async () => {
    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
    };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => true, // first byte already on the wire
    });

    await vi.advanceTimersByTimeAsync(0);

    expect(vi.getTimerCount()).toBe(0);
    expect(op).toHaveBeenCalledTimes(1);
    await expect(result).resolves.toEqual(attempt1);
  });

  it("resolves to an aborted outcome built from the signal reason when the signal fires during backoff", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5); // deterministic 50ms backoff window

    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
    };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const controller = new AbortController();
    const now = Date.now();
    const result = retry(op, {
      signal: controller.signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    // Attempt 1 resolves and the backoff sleep is scheduled.
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(1);

    // The wall-clock fires mid-backoff.
    controller.abort({ kind: "wall_clock_expired" });
    await vi.advanceTimersByTimeAsync(0);

    expect(op).toHaveBeenCalledTimes(1); // no second attempt
    await expect(result).resolves.toEqual({
      kind: "aborted",
      abort_kind: "wall_clock_expired",
    });
  });

  it("re-throws the original reason when the signal fires during backoff with an unrecognized reason", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5); // deterministic 50ms backoff window

    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
    };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const controller = new AbortController();
    const now = Date.now();
    const result = retry(op, {
      signal: controller.signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(1);

    const bogusReason = { kind: "totally_unknown" };
    // Attach the rejection handler BEFORE aborting: abort rejects synchronously,
    // and an unhandled window here surfaces as a run-failing unhandled error.
    const rejection = expect(result).rejects.toBe(bogusReason);
    controller.abort(bogusReason);
    await rejection;

    expect(op).toHaveBeenCalledTimes(1);
  });

  it("backs off a full-jitter fraction of the ceiling when no Retry-After is present", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
    };
    const attempt2: Outcome = {
      kind: "ok",
      status: 200,
      body_parsed: { ok: true },
    };

    const op = vi
      .fn<() => Promise<Outcome>>()
      .mockResolvedValueOnce(attempt1)
      .mockResolvedValueOnce(attempt2);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    // Full jitter: with Math.random() pinned to 0.5, the backoff is half the ceiling.
    const expectedBackoff = Math.floor(0.5 * backoffCeiling());

    await vi.advanceTimersByTimeAsync(0);
    expect(op).toHaveBeenCalledTimes(1); // backoff scheduled, not immediate

    await vi.advanceTimersByTimeAsync(expectedBackoff - 1);
    expect(op).toHaveBeenCalledTimes(1); // not before ~half the ceiling

    await vi.advanceTimersByTimeAsync(2);
    expect(op).toHaveBeenCalledTimes(2); // retry fires around half the ceiling

    await expect(result).resolves.toEqual(attempt2);
  });

  it("backs off a full-jitter fraction of the ceiling when the Retry-After is present but empty", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 429,
      body_raw: "rate limited",
      retry_after: "", // header present but empty — Headers.get returns "" for `Retry-After:` with no value
    };
    const attempt2: Outcome = {
      kind: "ok",
      status: 200,
      body_parsed: { ok: true },
    };

    const op = vi
      .fn<() => Promise<Outcome>>()
      .mockResolvedValueOnce(attempt1)
      .mockResolvedValueOnce(attempt2);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    // An empty Retry-After must fall back to jittered backoff, NOT an immediate
    // zero-wait retry. With Math.random pinned to 0.5, that is half the ceiling.
    const expectedBackoff = Math.floor(0.5 * backoffCeiling());

    await vi.advanceTimersByTimeAsync(0);
    expect(op).toHaveBeenCalledTimes(1); // backoff scheduled, not fired immediately

    await vi.advanceTimersByTimeAsync(expectedBackoff - 1);
    expect(op).toHaveBeenCalledTimes(1); // still waiting out the backoff

    await vi.advanceTimersByTimeAsync(2);
    expect(op).toHaveBeenCalledTimes(2); // retry fires around half the ceiling

    await expect(result).resolves.toEqual(attempt2);
  });

  it("treats a literal Retry-After: 0 as no usable backoff and jitters instead of retrying immediately", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 429,
      body_raw: "rate limited",
      retry_after: "0", // explicit zero: honoring it would mean a zero-wait retry, so it falls back to jitter
    };
    const attempt2: Outcome = {
      kind: "ok",
      status: 200,
      body_parsed: { ok: true },
    };

    const op = vi
      .fn<() => Promise<Outcome>>()
      .mockResolvedValueOnce(attempt1)
      .mockResolvedValueOnce(attempt2);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    const expectedBackoff = Math.floor(0.5 * backoffCeiling());

    await vi.advanceTimersByTimeAsync(0);
    expect(op).toHaveBeenCalledTimes(1); // not honored as an immediate retry

    await vi.advanceTimersByTimeAsync(expectedBackoff - 1);
    expect(op).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(2);
    expect(op).toHaveBeenCalledTimes(2);

    await expect(result).resolves.toEqual(attempt2);
  });

  it("does not retry a network failure that is not proven pre-send (post-send is idempotency-unsafe)", async () => {
    const attempt1: Outcome = { kind: "network_failed", pre_send_proven: false };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => false,
    });

    await vi.advanceTimersByTimeAsync(0);

    expect(vi.getTimerCount()).toBe(0); // not retry-eligible: no backoff scheduled
    expect(op).toHaveBeenCalledTimes(1);
    await expect(result).resolves.toEqual(attempt1);
  });

  it("does not fire the second attempt when firstByteFlushed flips true during the backoff", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    let flushed = false;
    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
    };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30_000,
      firstByteFlushed: () => flushed,
    });

    const expectedBackoff = Math.floor(0.5 * backoffCeiling());

    await vi.advanceTimersByTimeAsync(0);
    expect(op).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1); // backoff scheduled

    // First streamed byte reaches the client mid-backoff.
    flushed = true;

    await vi.advanceTimersByTimeAsync(expectedBackoff);
    expect(op).toHaveBeenCalledTimes(1); // getter re-read after sleep — no second attempt
    await expect(result).resolves.toEqual(attempt1);
  });

  it("skips the retry without sleeping when the jittered backoff alone would cross the deadline", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5); // ~50ms backoff (half the ceiling)

    const attempt1: Outcome = {
      kind: "upstream_error",
      status: 503,
      body_raw: "unavailable",
    };

    const op = vi.fn<() => Promise<Outcome>>().mockResolvedValue(attempt1);

    const now = Date.now();
    const result = retry(op, {
      signal: new AbortController().signal,
      deadlineAt: now + 30, // 30ms budget; the ~50ms backoff cannot fit, even with no Retry-After
      firstByteFlushed: () => false,
    });

    await vi.advanceTimersByTimeAsync(0);

    expect(vi.getTimerCount()).toBe(0); // backoff crosses the deadline → no sleep scheduled
    expect(op).toHaveBeenCalledTimes(1);
    await expect(result).resolves.toEqual(attempt1); // passthrough, not a 504/aborted
  });
});
