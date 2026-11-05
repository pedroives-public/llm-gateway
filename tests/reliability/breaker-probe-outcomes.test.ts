import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import type { StreamingAdapter } from "../../src/upstream/stream.js";

const OPEN_MS = 60_000;
const realSetImmediate = setImmediate;

type LogLine = Record<string, unknown>;

describe("breaker probe — every exit has a next step", () => {
  let unhandled: unknown[];
  let savedListeners: NodeJS.UnhandledRejectionListener[];

  // The real event loop, because fake timers replace setImmediate too and an
  // unhandled rejection is only reported once the microtasks have drained.
  const flushRejections = (): Promise<void> =>
    new Promise((resolve) => {
      realSetImmediate(resolve);
    });

  // Vitest's expect(...).resolves replaces the sentence with its own message,
  // so the throw is turned into a value that the rule's expect can name.
  const advanceReportingThrow = (ms: number): Promise<string> =>
    vi.advanceTimersByTimeAsync(ms).then(
      () => "nothing thrown",
      () => "thrown out of the timer",
    );

  const openBreaker = (breaker: {
    recordResult: (outcome: "FAILURE") => void;
  }): void => {
    for (let i = 0; i < 5; i++) breaker.recordResult("FAILURE");
  };

  // A logger that throws on the first line matching `when`, and only there.
  const throwingOnce = (when: (line: LogLine) => boolean) => {
    let thrown = false;
    return {
      info: (line: object): void => {
        if (!thrown && when(line as LogLine)) {
          thrown = true;
          throw new Error("logger failed");
        }
      },
      error: (): void => {},
    };
  };

  const toState =
    (to: string) =>
    (line: LogLine): boolean =>
      line.event === "cb_state_change" && line.to === to;

  const accepted = (body: string): StreamingAdapter => ({
    kind: "accepted_stream",
    status: 200,
    reader: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(body));
        controller.close();
      },
    }).getReader(),
  });

  beforeEach(() => {
    vi.resetModules();
    vi.useFakeTimers();
    unhandled = [];
    // Vitest's own listener would report these as a run error with no
    // sentence; here the cell is the ruler, and it names the rule.
    savedListeners = process.listeners("unhandledRejection");
    process.removeAllListeners("unhandledRejection");
    process.on("unhandledRejection", (reason) => {
      unhandled = [...unhandled, reason];
    });
  });

  afterEach(async () => {
    vi.useRealTimers();
    await flushRejections();
    process.removeAllListeners("unhandledRejection");
    for (const listener of savedListeners) {
      process.on("unhandledRejection", listener);
    }
  });

  it("returns to OPEN with a new probe when the runner throws", async () => {
    const { createCircuitBreaker } = await import(
      "../../src/reliability/circuit-breaker.js"
    );
    const runProbe = vi.fn((): void => {
      throw new Error("runner failed");
    });
    const breaker = createCircuitBreaker({ info: () => {} }, runProbe);
    openBreaker(breaker);

    expect(
      await advanceReportingThrow(OPEN_MS),
      "nothing above the timer catches: a throw there must be handled inside it",
    ).toBe("nothing thrown");

    expect(
      breaker.getState(),
      "a runner that throws must not strand HALF_OPEN with no timer armed",
    ).toBe("OPEN");
    await vi.advanceTimersByTimeAsync(OPEN_MS);
    expect(
      runProbe,
      "recovery must not depend on tenant traffic: a new probe runs 60 s later",
    ).toHaveBeenCalledTimes(2);
  });

  it("returns to OPEN with a new probe when logging OPEN to HALF_OPEN throws", async () => {
    const { createCircuitBreaker } = await import(
      "../../src/reliability/circuit-breaker.js"
    );
    const runProbe = vi.fn();
    const breaker = createCircuitBreaker(
      throwingOnce(toState("HALF_OPEN")),
      runProbe,
    );
    openBreaker(breaker);

    expect(
      await advanceReportingThrow(OPEN_MS),
      "nothing above the timer catches: a throw there must be handled inside it",
    ).toBe("nothing thrown");

    expect(
      breaker.getState(),
      "a failed log line must not strand HALF_OPEN with no timer armed",
    ).toBe("OPEN");
    await vi.advanceTimersByTimeAsync(OPEN_MS);
    expect(
      runProbe,
      "recovery must not depend on tenant traffic: a new probe runs 60 s later",
    ).toHaveBeenCalledTimes(1);
  });

  it("returns to OPEN with a new probe when an async runner rejects without voting", async () => {
    const { createCircuitBreaker } = await import(
      "../../src/reliability/circuit-breaker.js"
    );
    // A plain function on purpose: vi.fn attaches its own handlers to a returned
    // promise, and the rejection would never be reported as unhandled.
    let attempts = 0;
    const runProbe = async (): Promise<void> => {
      attempts += 1;
      throw new Error("runner failed");
    };
    const breaker = createCircuitBreaker({ info: () => {} }, runProbe);
    openBreaker(breaker);

    await vi.advanceTimersByTimeAsync(OPEN_MS);
    await flushRejections();

    expect(
      unhandled,
      "an unhandled rejection takes the process down, and every tenant with it",
    ).toEqual([]);
    expect(
      breaker.getState(),
      "a probe with no vote must not strand HALF_OPEN with no timer armed",
    ).toBe("OPEN");
    await vi.advanceTimersByTimeAsync(OPEN_MS);
    expect(
      attempts,
      "recovery must not depend on tenant traffic: a new probe runs 60 s later",
    ).toBe(2);
  });

  it("returns to OPEN with a new probe when an async runner resolves without voting", async () => {
    const { createCircuitBreaker } = await import(
      "../../src/reliability/circuit-breaker.js"
    );
    let attempts = 0;
    const runProbe = async (): Promise<void> => {
      attempts += 1;
    };
    const breaker = createCircuitBreaker({ info: () => {} }, runProbe);
    openBreaker(breaker);

    await vi.advanceTimersByTimeAsync(OPEN_MS);

    expect(
      breaker.getState(),
      "a completed runner that never votes must not strand HALF_OPEN",
    ).toBe("OPEN");
    await vi.advanceTimersByTimeAsync(OPEN_MS);
    expect(
      attempts,
      "recovery must not depend on tenant traffic: a new probe runs 60 s later",
    ).toBe(2);
  });

  it.each([
    {
      scene: "logging HALF_OPEN to OPEN throws during the vote",
      adapter: (): StreamingAdapter => ({
        kind: "upstream_error",
        status: 503,
        body_raw: "",
      }),
      when: toState("OPEN"),
      state: "OPEN",
    },
    {
      scene: "logging HALF_OPEN to CLOSED throws during the vote",
      adapter: (): StreamingAdapter => accepted("data: [DONE]\n\n"),
      when: toState("CLOSED"),
      state: "CLOSED",
    },
  ])("leaves no unhandled rejection when $scene", async ({ adapter, when, state }) => {
    const { createCircuitBreaker } = await import(
      "../../src/reliability/circuit-breaker.js"
    );
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const breaker = createCircuitBreaker(
      // The entry to OPEN itself logs too, so the line that throws is the one
      // written during the probe's vote: only the later match counts.
      (() => {
        const failing = throwingOnce(when);
        let armed = false;
        return {
          info: (line: object): void => {
            if ((line as LogLine).to === "HALF_OPEN") armed = true;
            if (armed) failing.info(line);
          },
        };
      })(),
      createProbeRunner({
        streaming: async () => adapter(),
        model: "probe-model",
        log: { info: () => {}, error: () => {} },
      }),
    );
    openBreaker(breaker);

    await vi.advanceTimersByTimeAsync(OPEN_MS);
    await flushRejections();

    expect(
      unhandled,
      "an unhandled rejection takes the process down, and every tenant with it",
    ).toEqual([]);
    expect(
      breaker.getState(),
      "the vote already moved the breaker: a failed log line must not undo it",
    ).toBe(state);
  });

  it("leaves no unhandled rejection when logging a caught error throws", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const vote = vi.fn();
    const runProbe = createProbeRunner({
      streaming: async () => {
        throw new Error("boom");
      },
      model: "probe-model",
      log: { ...throwingOnce(() => true) },
    });

    void runProbe(vote);
    await vi.advanceTimersByTimeAsync(0);
    await flushRejections();

    expect(vote, "the vote comes before the log line").toHaveBeenCalledWith(
      "FAILURE",
    );
    expect(
      unhandled,
      "an unhandled rejection takes the process down, and every tenant with it",
    ).toEqual([]);
  });
});
