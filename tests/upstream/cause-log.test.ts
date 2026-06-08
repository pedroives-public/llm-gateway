import { extractCauseLog } from "../../src/upstream/cause-log.js";
import { describe, it, expect } from "vitest";

describe("extractCauseLog", () => {
  it("projects only { cause_code, cause_name } from a native fetch connect rejection", () => {
    const err = Object.assign(new TypeError("fetch failed"), {
      cause: { code: "ECONNREFUSED", name: "Error" },
    });
    const result = extractCauseLog(err);
    expect(result).toEqual({ cause_code: "ECONNREFUSED", cause_name: "Error" });
  });

  it("does not leak headers/body/auth from the cause", () => {
    // Threat model: a real upstream rejection can carry an auth header and a PII
    // body on its `cause`. This test locks the whitelist — `toEqual` is exact, so a
    // future `{ ...err.cause }`-style refactor that leaked those keys fails here.
    const err = Object.assign(new TypeError("fetch failed"), {
      cause: {
        code: "ECONNREFUSED",
        name: "Error",
        headers: { authorization: "Bearer sk-SECRET" },
        body: "...PII...",
      },
    });

    const result = extractCauseLog(err);
    expect(result).toEqual({ cause_code: "ECONNREFUSED", cause_name: "Error" });
  });

  it.each([
    { input: null, label: "null" },
    { input: undefined, label: "undefined" },
    { input: "not an error", label: "string" },
    { input: new Error("no cause"), label: "error without cause" },
  ])(
    "returns the UNKNOWN sentinel (never throws) for degenerate input: $label",
    ({ input }) => {
      const result = extractCauseLog(input);
      expect(result).toEqual({ cause_code: "UNKNOWN", cause_name: "UNKNOWN" });
    },
  );
});
