import { describe, it, expect, expectTypeOf } from "vitest";
import * as timeouts from "../../src/reliability/timeouts.js";

// Single-abort-point pin: the AbortController and its typed abort funnel are
// module-private. The wall-clock helper hands out only { signal, clear }; the
// total-duration helper adds exactly two narrowed abort capabilities, one for
// the client-departure observer and one for the terminal block, and nothing
// wider. Contract growth (e.g. a leaked controller) must fail compilation or
// this suite.

describe("timeouts module — public contract pin", () => {
  it("armWallClockTimeout's return type is EXACTLY { signal, clear }", () => {
    expectTypeOf(timeouts.armWallClockTimeout).returns.toEqualTypeOf<{
      signal: AbortSignal;
      clear: () => void;
    }>();
  });

  it("armTotalDurationTimeout's return type is EXACTLY { signal, clear } plus the two narrowed abort capabilities", () => {
    expectTypeOf(timeouts.armTotalDurationTimeout).returns.toEqualTypeOf<{
      signal: AbortSignal;
      clear: () => void;
      abortForClientGoneIfUndecided: () => void;
      abortForTerminalAfterDecision: (
        kind: "total_timeout" | "client_gone" | "parser_buffer_cap",
      ) => void;
    }>();
  });

  it("the module's runtime export surface is exactly the intended names", () => {
    // A set, not a list: the contract is which names exist, not the order
    // the module declares them in.
    expect(new Set(Object.keys(timeouts))).toEqual(
      new Set(["armWallClockTimeout", "armTotalDurationTimeout"]),
    );
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

  it("armTotalDurationTimeout's returned object carries exactly the { signal, clear } keys plus the two narrowed abort capabilities at runtime", () => {
    const armedTimeout = timeouts.armTotalDurationTimeout(
      Date.now() + 60_000,
      () => false,
    );

    try {
      expect(
        new Set(Object.keys(armedTimeout)),
        "the helper hands out the signal, clear and the two narrowed capabilities, never the controller or the funnel",
      ).toEqual(
        new Set([
          "signal",
          "clear",
          "abortForClientGoneIfUndecided",
          "abortForTerminalAfterDecision",
        ]),
      );
    } finally {
      armedTimeout.clear();
    }
  });
});
