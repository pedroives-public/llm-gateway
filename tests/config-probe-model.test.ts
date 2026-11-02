import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getProbeModel } from "../src/config.js";

describe("getProbeModel", () => {
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env["BREAKER_PROBE_MODEL"];
  });

  afterEach(() => {
    if (saved === undefined) {
      delete process.env["BREAKER_PROBE_MODEL"];
    } else {
      process.env["BREAKER_PROBE_MODEL"] = saved;
    }
  });

  it("refuses when the setting is absent", () => {
    delete process.env["BREAKER_PROBE_MODEL"];

    expect(
      () => getProbeModel(),
      "the probe has no default model: the deployment must name it",
    ).toThrow("BREAKER_PROBE_MODEL must name a model");
  });

  // The whole message is pinned: it carries nothing of the received value, so
  // a secret pasted into the wrong variable never reaches the boot log.
  it.each([[""], [" "], [" gpt-4o-mini"], ["gpt-4o-mini "]])(
    "refuses %j",
    (value) => {
      process.env["BREAKER_PROBE_MODEL"] = value;

      expect(
        () => getProbeModel(),
        "an empty or padded model must fail boot, never reach the upstream as the probe's model",
      ).toThrow(/^BREAKER_PROBE_MODEL must name a model$/);
    },
  );

  it("returns the configured model unchanged", () => {
    process.env["BREAKER_PROBE_MODEL"] = "gpt-4o-mini";

    expect(getProbeModel()).toBe("gpt-4o-mini");
  });
});
