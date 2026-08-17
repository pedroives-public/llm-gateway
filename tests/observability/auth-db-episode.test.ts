import { describe, expect, it } from "vitest";
import {
  createAuthDbEpisodeDetector,
  type AuthDbEpisodeDetector,
} from "../../src/observability/auth-db-episode.js";

// Each cell drives a full event sequence and asserts the complete transition
// trace, so a spurious or missed open fails by position. Threshold and window
// are test-local: cells pin behavior relative to the configured values, so
// the production constants never touch this suite.

const THRESHOLD = 3;
const WINDOW_MS = 1_000;

function makeDetector(): AuthDbEpisodeDetector {
  return createAuthDbEpisodeDetector({
    threshold: THRESHOLD,
    windowMs: WINDOW_MS,
  });
}

function failures(d: AuthDbEpisodeDetector, atMs: number[]): boolean[] {
  return atMs.map((t) => d.recordFailure(t));
}

describe("auth DB episode detector — open/close state machine", () => {
  it("opens the episode exactly at the failure that crosses the threshold", () => {
    const d = makeDetector();

    expect(
      failures(d, [0, 100, 200]),
      "episode did not open at the threshold-crossing failure: the availability invariant lost its only summons",
    ).toEqual([false, false, true]);
  });

  it("failures spread wider than the window never form an episode", () => {
    const d = makeDetector();

    // Each failure sits alone in any trailing window: hiccups, not a sequence.
    expect(
      failures(d, [0, 1_200, 2_400]),
      "isolated failures outside the window opened an episode: threshold window not honored (false positive)",
    ).toEqual([false, false, false]);
  });

  it("an open episode emits once: later failures are the same episode", () => {
    const d = makeDetector();

    expect(
      failures(d, [0, 100, 200, 300, 400]),
      "an already-open episode re-emitted on a later failure: the alert must fire only on the closed→open transition",
    ).toEqual([false, false, true, false, false]);
  });

  it("recovery evidence closes; the next crossing is a NEW episode that emits again", () => {
    const d = makeDetector();
    expect(failures(d, [0, 100, 200])).toEqual([false, false, true]);

    d.recordSuccess(300);

    expect(
      failures(d, [400, 500, 600]),
      "second episode after recovery did not emit: at-least-once PER EPISODE violated (this alert has no once-per-process dedup to lean on)",
    ).toEqual([false, false, true]);
  });

  it("interleaved successes do not blind the window: flapping still crosses the threshold", () => {
    const d = makeDetector();

    // Successes close open episodes; they do not erase pre-episode evidence.
    const trace: boolean[] = [];
    trace.push(d.recordFailure(100));
    d.recordSuccess(150);
    trace.push(d.recordFailure(200));
    d.recordSuccess(250);
    trace.push(d.recordFailure(300));

    expect(
      trace,
      "a pre-episode success reset the failure window: the flapping DB mode can never summon the operator",
    ).toEqual([false, false, true]);
  });

  it("absence of traffic never closes: after a quiet gap it is still the same episode", () => {
    const d = makeDetector();
    expect(failures(d, [0, 100, 200])).toEqual([false, false, true]);

    // Ten quiet hours, no recovery evidence: still the same open episode.
    const late = 200 + 10 * 60 * 60 * 1_000;
    expect(
      failures(d, [late, late + 100, late + 200]),
      "a quiet gap closed the episode by itself: only recovery evidence may close",
    ).toEqual([false, false, false]);
  });
});
