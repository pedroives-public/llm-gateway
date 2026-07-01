import { describe, it, expect, expectTypeOf } from "vitest";
import * as timeouts from "../../src/reliability/timeouts.js";

// Single-abort-point pin: the AbortController and its typed abort funnel are
// module-private — consumers get only { signal, clear }. Contract growth
// (e.g. a leaked controller) must fail compilation or this suite.

describe("timeouts module — public contract pin", () => {
  it("armWallClockTimeout's return type is EXACTLY { signal, clear }", () => {
    expectTypeOf(timeouts.armWallClockTimeout).returns.toEqualTypeOf<{
      signal: AbortSignal;
      clear: () => void;
    }>();
  });

  it("the module's runtime export surface is exactly the intended names", () => {
    expect(Object.keys(timeouts)).toEqual(["armWallClockTimeout"]);
  });

  // The type pin above guards the DECLARED contract; this guards the runtime
  // object — a leak smuggled past tsc via a cast still fails here.
  it("the returned object carries exactly the { signal, clear } keys at runtime", () => {
    const armedTimeout = timeouts.armWallClockTimeout(60_000);

    try {
      expect(new Set(Object.keys(armedTimeout))).toEqual(
        new Set(["signal", "clear"]),
      );
    } finally {
      armedTimeout.clear();
    }
  });
});
