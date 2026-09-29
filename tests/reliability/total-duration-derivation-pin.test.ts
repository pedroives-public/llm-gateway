import { describe, it, expect } from "vitest";
import { STREAM_TOTAL_DURATION_MS } from "../../src/config.js";

// Pins the streaming total-duration deadline to its derivation corridor, one
// inequality per row and never an equality: the value may move inside the
// corridor, but whoever moves it past a bound, or re-measures a slower
// legitimate completion, is forced to re-derive it. The bounds are literal on
// purpose: a bound computed here (1.5 x 165 s) would move with a typo in the
// same expression it is meant to check.
const corridor: {
  claim: string;
  holds: (deadlineMs: number, claim: string) => void;
}[] = [
  {
    claim:
      "covers the slowest legitimate completion measured (headers at 133 s + 32 s of generation = 165 s)",
    holds: (t, claim) => expect(t, claim).toBeGreaterThan(165_000),
  },
  {
    claim: "keeps a 1.5x margin over that completion (247.5 s)",
    holds: (t, claim) => expect(t, claim).toBeGreaterThanOrEqual(247_500),
  },
  {
    claim:
      "fires strictly before the outbound dispatcher's 300 s headersTimeout",
    holds: (t, claim) => expect(t, claim).toBeLessThan(300_000),
  },
];

describe("streaming total-duration derivation pin", () => {
  it.each(corridor)("the deadline $claim", ({ claim, holds }) => {
    holds(STREAM_TOTAL_DURATION_MS, claim);
  });
});
