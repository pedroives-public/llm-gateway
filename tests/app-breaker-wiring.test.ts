import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { fakeAuthDb } from "./helpers/fake-auth-db.js";

// The app's breaker is built inside buildApp, so the only seam is the
// factory: it is wrapped to record what the app hands it and when the app
// closes it, while the real breaker still does the work.
const seen = vi.hoisted(() => ({
  runners: [] as unknown[],
  closes: 0,
}));

vi.mock("../src/reliability/circuit-breaker.js", async (importOriginal) => {
  const real =
    await importOriginal<
      typeof import("../src/reliability/circuit-breaker.js")
    >();
  return {
    ...real,
    createCircuitBreaker: (
      ...args: Parameters<typeof real.createCircuitBreaker>
    ) => {
      seen.runners = [...seen.runners, args[1]];
      const breaker = real.createCircuitBreaker(...args);
      return {
        ...breaker,
        close: () => {
          seen.closes += 1;
          breaker.close();
        },
      };
    },
  };
});

describe("buildApp — breaker probe wiring", () => {
  it("gives the breaker a probe runner and closes it with the app", async () => {
    const { buildApp } = await import("../src/app.js");
    const app = await buildApp({ logger: false, db: fakeAuthDb(randomUUID()) });

    expect(
      seen.runners.map((runner) => typeof runner),
      "without a probe runner the app's breaker can never leave OPEN",
    ).toEqual(["function"]);
    expect(seen.closes, "the breaker stays live while the app runs").toBe(0);

    await app.close();

    expect(seen.closes, "closing the app stops the breaker").toBe(1);
  });
});
