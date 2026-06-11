import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { armWallClockTimeout } from "../../src/reliability/timeouts.js";

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
