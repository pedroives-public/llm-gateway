import http from "node:http";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import type { StreamingUpstream } from "../../src/upstream/openai.js";
import type { StreamingAdapter } from "../../src/upstream/stream.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";

// Every way a recovery probe can end, through the real adapter and a real
// socket: the adapter decides which outcome each response becomes, so a fake
// seam would pin the test's own mapping. The modules are reloaded for every
// cell because an operational alert fires once per process.

const OPEN_MS = 60_000;
const CANARY = "upstream-canary-9c1d";
const QUOTA_BODY = JSON.stringify({ error: { code: "insufficient_quota" } });
const ERROR_BODY = JSON.stringify({ error: { message: "refused" } });
const realSetImmediate = setImmediate;

// The app is not closing in these scenes: a signal that never aborts.
const NOT_CLOSING = new AbortController().signal;

type LogLine = Record<string, unknown>;

type UpstreamResponse = {
  status: number;
  headers: Record<string, string>;
  body: string;
};

function capturingLog() {
  const lines: LogLine[] = [];
  const push = (line: object): void => {
    lines.push(line as LogLine);
  };
  return {
    lines,
    log: { info: push, error: push },
    alerts: () =>
      lines
        .filter((line) => line.event === "operational_alert")
        .map((line) => line.alert),
  };
}

async function withUpstream(
  response: UpstreamResponse,
  scene: (client: {
    streaming: StreamingUpstream;
  }) => Promise<void>,
): Promise<void> {
  const { createOpenAIClient } = await import("../../src/upstream/openai.js");
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(response.status, response.headers);
      res.end(response.body);
    });
  });
  const { port, close } = await listenEphemeral(server);
  try {
    await scene(
      createOpenAIClient({
        apiKey: "deployment-key",
        baseURL: `http://127.0.0.1:${port}`,
      }),
    );
  } finally {
    await close();
  }
}

const json = (status: number, body = ERROR_BODY): UpstreamResponse => ({
  status,
  headers: { "content-type": "application/json" },
  body,
});
const sse = (body: string): UpstreamResponse => ({
  status: 200,
  headers: { "content-type": "text/event-stream" },
  body,
});

describe("breaker probe — outcome table", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it.each([
    { scene: "[DONE] inside the clock", response: sse("data: [DONE]\n\n"), outcome: "SUCCESS", alerts: [], because: "[DONE] is the evidence of recovery and raises nothing" },
    { scene: "500", response: json(500), outcome: "FAILURE", alerts: [], because: "a 5xx is the upstream failing, which the breaker already reports" },
    { scene: "503", response: json(503), outcome: "FAILURE", alerts: [], because: "a 5xx is the upstream failing, which the breaker already reports" },
    { scene: "500 with a body over the size cap", response: json(500, "x".repeat(2 * 1024 * 1024)), outcome: "FAILURE", alerts: [], because: "an error body too large to read proves no recovery" },
    { scene: "429 rate limit", response: json(429), outcome: "FAILURE", alerts: [], because: "a plain 429 says not now, and summons nobody" },
    { scene: "429 insufficient_quota", response: json(429, QUOTA_BODY), outcome: "FAILURE", alerts: ["upstream_quota_exhausted"], because: "no quota needs the operator, and retrying in 60 s will not fix it" },
    { scene: "401", response: json(401), outcome: "FAILURE", alerts: ["upstream_auth_failure"], because: "a rejected deployment credential needs the operator" },
    { scene: "403", response: json(403), outcome: "FAILURE", alerts: ["upstream_probe_access_denied"], because: "a denied account keeps every tenant at 503 and needs an alert of its own" },
    { scene: "400", response: json(400), outcome: "SUCCESS", alerts: ["upstream_probe_rejected"], because: "a refused probe config must not hold every tenant in 503" },
    { scene: "404", response: json(404), outcome: "SUCCESS", alerts: ["upstream_probe_rejected"], because: "a refused probe config must not hold every tenant in 503" },
    { scene: "413", response: json(413), outcome: "SUCCESS", alerts: ["upstream_probe_rejected"], because: "a refused probe config must not hold every tenant in 503" },
    { scene: "422", response: json(422), outcome: "SUCCESS", alerts: ["upstream_probe_rejected"], because: "a refused probe config must not hold every tenant in 503" },
    { scene: "408", response: json(408), outcome: "FAILURE", alerts: [], because: "a 4xx that says not now proves nothing about recovery" },
    { scene: "409", response: json(409), outcome: "FAILURE", alerts: [], because: "a 4xx that says not now proves nothing about recovery" },
    { scene: "418, a status nobody named", response: json(418), outcome: "FAILURE", alerts: [], because: "a status not named proves nothing about recovery" },
    { scene: "300, a 3xx fetch does not follow", response: json(300), outcome: "FAILURE", alerts: ["upstream_redirect_blocked"], because: "an endpoint that answers 3xx is a deployment fault the operator must fix" },
    { scene: "SSE 200 without [DONE]", response: sse("data: {}\n\n"), outcome: "FAILURE", alerts: [], because: "a stream that ends without [DONE] proves no recovery" },
    { scene: "200 that is not SSE", response: json(200, "{}"), outcome: "FAILURE", alerts: [], because: "a 2xx without [DONE] proves no recovery" },
    { scene: "302 redirect", response: { status: 302, headers: { location: "http://127.0.0.1:1/" }, body: "" }, outcome: "FAILURE", alerts: ["upstream_redirect_blocked"], because: "an endpoint that redirects is a deployment fault the operator must fix" },
  ])(
    "$scene votes $outcome and alerts $alerts",
    async ({ response, outcome, alerts, because }) => {
      const { createProbeRunner } = await import(
        "../../src/reliability/breaker-probe.js"
      );
      const captured = capturingLog();
      const vote = vi.fn();

      await withUpstream(response, async ({ streaming }) => {
        await createProbeRunner({
          streaming,
          model: "probe-model",
          log: captured.log,
        })(vote, NOT_CLOSING);
      });

      expect(vote, "a probe votes exactly once").toHaveBeenCalledTimes(1);
      expect(vote, because).toHaveBeenCalledWith(outcome);
      expect(captured.alerts(), because).toEqual(alerts);
    },
  );

  it("votes FAILURE with no alert on a network failure", async () => {
    const { createOpenAIClient } = await import(
      "../../src/upstream/openai.js"
    );
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    // Listening and closing leaves a port that refuses the connection.
    const { port, close } = await listenEphemeral(http.createServer());
    await close();
    const captured = capturingLog();
    const vote = vi.fn();

    await createProbeRunner({
      streaming: createOpenAIClient({
        apiKey: "deployment-key",
        baseURL: `http://127.0.0.1:${port}`,
      }).streaming,
      model: "probe-model",
      log: captured.log,
    })(vote, NOT_CLOSING);

    expect(vote, "a refused connection proves no recovery").toHaveBeenCalledWith(
      "FAILURE",
    );
    expect(captured.alerts(), "a network failure summons nobody").toEqual([]);
  });

  it("raises a probe alert once per process, across attempts", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const captured = capturingLog();

    await withUpstream(json(403), async ({ streaming }) => {
      const runProbe = createProbeRunner({
        streaming,
        model: "probe-model",
        log: captured.log,
      });
      await runProbe(vi.fn(), NOT_CLOSING);
      await runProbe(vi.fn(), NOT_CLOSING);
    });

    expect(
      captured.alerts(),
      "a probe repeats every 60 s and must summon the operator only once",
    ).toEqual(["upstream_probe_access_denied"]);
  });

  // Alerts fire once per process by name: if a refused config and a denied
  // account shared a name, the earlier one would silence the outage.
  it("still alerts a denied account after a refused probe config alerted", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const captured = capturingLog();
    const votes: string[] = [];

    for (const response of [json(400), json(403)]) {
      await withUpstream(response, async ({ streaming }) => {
        await createProbeRunner({
          streaming,
          model: "probe-model",
          log: captured.log,
        })((outcome) => {
          votes.push(outcome);
        }, NOT_CLOSING);
      });
    }

    // Positive control: the 400 closed and the 403 kept OPEN, so the scene
    // reached both decisions.
    expect(votes, "the scene must reach a refused config and a denied account").toEqual([
      "SUCCESS",
      "FAILURE",
    ]);
    expect(
      captured.alerts(),
      "a 403 keeps every tenant at 503, so an earlier refused-config alert must not silence it",
    ).toEqual(["upstream_probe_rejected", "upstream_probe_access_denied"]);
  });

  it("names the probe in the alert line, and nothing else", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const captured = capturingLog();

    await withUpstream(json(401), async ({ streaming }) => {
      await createProbeRunner({
        streaming,
        model: "probe-model",
        log: captured.log,
      })(vi.fn(), NOT_CLOSING);
    });

    expect(
      captured.lines.filter((line) => line.event === "operational_alert"),
      "no tenant request stands behind a probe alert, so its req_id names the probe",
    ).toEqual([
      {
        event: "operational_alert",
        alert: "upstream_auth_failure",
        req_id: "breaker_probe",
      },
    ]);
  });
});

describe("breaker probe — nothing from the upstream reaches the logs", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it.each([
    { scene: "400", status: 400 },
    { scene: "401", status: 401 },
    { scene: "403", status: 403 },
    { scene: "404", status: 404 },
    { scene: "413", status: 413 },
    { scene: "422", status: 422 },
    { scene: "429", status: 429 },
    { scene: "500", status: 500 },
    { scene: "418", status: 418 },
  ])("keeps the body of a $scene out of every line", async ({ status }) => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const captured = capturingLog();
    let bodySeen = "";

    await withUpstream(
      json(status, JSON.stringify({ error: { message: CANARY } })),
      async ({ streaming }) => {
        await createProbeRunner({
          streaming: async (...args) => {
            const result = await streaming(...args);
            if (result.kind === "upstream_error") bodySeen = result.body_raw;
            return result;
          },
          model: "probe-model",
          log: captured.log,
        })(vi.fn(), NOT_CLOSING);
      },
    );

    // Positive control: the canary did reach the adapter's result, so its
    // absence from the lines below is a reading and not a blind ruler.
    expect(bodySeen, "the upstream's body must carry the canary").toContain(
      CANARY,
    );
    expect(
      JSON.stringify(captured.lines),
      "an upstream body may echo anything, and must never reach a log line",
    ).not.toContain(CANARY);
  });

  it("keeps a rejection's message out of every line", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const captured = capturingLog();

    await createProbeRunner({
      streaming: async () => {
        throw new Error(CANARY, { cause: new Error(CANARY) });
      },
      model: "probe-model",
      log: captured.log,
    })(vi.fn(), NOT_CLOSING);

    expect(
      JSON.stringify(captured.lines),
      "an error's message never reaches a log line",
    ).not.toContain(CANARY);
  });
});

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
    const breaker = createCircuitBreaker({ info: () => {}, error: () => {} }, runProbe);
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
    const breaker = createCircuitBreaker({ info: () => {}, error: () => {} }, runProbe);
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
    const breaker = createCircuitBreaker({ info: () => {}, error: () => {} }, runProbe);
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
          error: failing.error,
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

  type VoteLogFailureRow = {
    scene: string;
    response: () => StreamingAdapter;
    when: (line: LogLine) => boolean;
    alerts: string[];
    because: string;
  };

  // Records every line, and throws once on the first line matching `when`
  // written after OPEN -> HALF_OPEN: the entry to OPEN logs too, and only a
  // line written during the probe's vote counts.
  const recordingThrowingOnce = (when: (line: LogLine) => boolean) => {
    let lines: LogLine[] = [];
    let armed = false;
    let thrown = false;
    const write = (line: object): void => {
      const entry = line as LogLine;
      if (entry.to === "HALF_OPEN") armed = true;
      if (armed && !thrown && when(entry)) {
        thrown = true;
        throw new Error("logger failed");
      }
      lines = [...lines, entry];
    };
    return {
      log: { info: write, error: write },
      thrown: () => thrown,
      alerts: () =>
        lines
          .filter((line) => line.event === "operational_alert")
          .map((line) => line.alert),
    };
  };

  const voteLogFailureRows: VoteLogFailureRow[] = [
    {
      response: () => ({ kind: "upstream_error", status: 401, body_raw: "" }),
      when: toState("OPEN"),
      alerts: ["upstream_auth_failure"],
      scene: "the HALF_OPEN to OPEN line throws during a 401 vote",
      because: "a breaker line that fails during the vote must not cost the operator the alert",
    },
    {
      response: () => ({ kind: "upstream_error", status: 403, body_raw: "" }),
      when: (line) => line.event === "breaker_probe_result" && line.outcome === "FAILURE",
      alerts: ["upstream_probe_access_denied"],
      scene: "the breaker_probe_result line throws during a 403 vote",
      because: "a breaker line that fails during the vote must not cost the operator the alert",
    },
    {
      response: () => ({ kind: "upstream_error", status: 400, body_raw: "" }),
      when: toState("CLOSED"),
      alerts: ["upstream_probe_rejected"],
      scene: "the HALF_OPEN to CLOSED line throws during a 400 vote",
      because: "a breaker line that fails during the vote must not cost the operator the alert",
    },
  ];

  // The breaker and the runner share the app's logger, and the runner writes
  // the alert after the vote returns: a breaker line that fails during the
  // vote must not cost the operator the alert.
  it.each(voteLogFailureRows)(
    "still alerts $alerts when $scene",
    async ({ response, when, alerts, because }) => {
      const { createCircuitBreaker } = await import(
        "../../src/reliability/circuit-breaker.js"
      );
      const { createProbeRunner } = await import(
        "../../src/reliability/breaker-probe.js"
      );
      const shared = recordingThrowingOnce(when);
      const breaker = createCircuitBreaker(
        shared.log,
        createProbeRunner({
          streaming: async () => response(),
          model: "probe-model",
          log: shared.log,
        }),
      );
      openBreaker(breaker);

      await vi.advanceTimersByTimeAsync(OPEN_MS);
      await flushRejections();

      // Positive control: without the throw, a missing alert would say
      // nothing about the rule.
      expect(
        shared.thrown(),
        "the scene must make the chosen breaker line throw",
      ).toBe(true);
      expect(shared.alerts(), because).toEqual(alerts);
    },
  );

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

    void runProbe(vote, NOT_CLOSING);
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

  it("leaves no unhandled rejection when logging the alert throws", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const vote = vi.fn();
    let thrown = 0;
    const runProbe = createProbeRunner({
      streaming: async (): Promise<StreamingAdapter> => ({
        kind: "upstream_error",
        status: 401,
        body_raw: "",
      }),
      model: "probe-model",
      log: {
        info: (): void => {},
        error: (line: object): void => {
          if ((line as LogLine).event === "operational_alert") {
            thrown += 1;
            throw new Error("logger failed");
          }
        },
      },
    });

    void runProbe(vote, NOT_CLOSING);
    await vi.advanceTimersByTimeAsync(0);
    await flushRejections();

    expect(thrown, "the scene must make the alert line throw").toBe(1);
    expect(vote, "the vote comes before the alert line").toHaveBeenCalledWith(
      "FAILURE",
    );
    expect(
      unhandled,
      "an unhandled rejection takes the process down, and every tenant with it",
    ).toEqual([]);
  });

  // A body that fails mid-read leaves the stream errored, so the cleanup's
  // cancel rejects too, and its observation line is the one that throws.
  it("keeps the caught error's line, and settles, when logging a rejected cancel throws", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const vote = vi.fn();
    let lines: LogLine[] = [];
    let thrown = 0;
    const write = (line: object): void => {
      const entry = line as LogLine;
      if (entry.event === "upstream_cancel_rejected") {
        thrown += 1;
        throw new Error("logger failed");
      }
      lines = [...lines, entry];
    };
    const socketReset = new TypeError("terminated", {
      cause: Object.assign(new Error("other side closed"), {
        code: "UND_ERR_SOCKET",
      }),
    });
    const runProbe = createProbeRunner({
      streaming: async (): Promise<StreamingAdapter> => ({
        kind: "accepted_stream",
        status: 200,
        reader: new ReadableStream<Uint8Array>({
          start(controller) {
            controller.error(socketReset);
          },
        }).getReader(),
      }),
      model: "probe-model",
      log: { info: write, error: write },
    });

    let settled = "pending";
    void runProbe(vote, NOT_CLOSING).then(
      () => {
        settled = "resolved";
      },
      () => {
        settled = "rejected";
      },
    );
    await vi.advanceTimersByTimeAsync(0);
    await flushRejections();

    // Positive control: without the throw, a present line would say nothing
    // about the rule.
    expect(thrown, "the scene must make the cancel-rejected line throw").toBe(1);
    expect(vote, "the vote comes before the cleanup").toHaveBeenCalledWith(
      "FAILURE",
    );
    expect(
      lines.filter((line) => line.event === undefined),
      "a failed cleanup line must not cost the operator the caught error's line",
    ).toEqual([
      { err_name: "TypeError", cause_code: "UND_ERR_SOCKET", cause_name: "Error" },
    ]);
    expect(
      settled,
      "a probe that has voted settles its attempt even when a cleanup line fails",
    ).toBe("resolved");
  });
});

describe("breaker probe — closing the app", () => {
  // Well under Vitest's 5 s test timeout, and far above the few microtask
  // turns an abort takes to settle the runner.
  const CLOSE_DEADLINE_MS = 1_000;

  beforeEach(() => {
    vi.resetModules();
  });

  it("hands the close's abort to the upstream call in flight", async () => {
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const closing = new AbortController();
    let upstreamSignal: AbortSignal | undefined;
    const vote = vi.fn();

    const run = createProbeRunner({
      // An upstream that never answers: only an abort ends the call.
      streaming: (_body, signal) => {
        upstreamSignal = signal;
        return new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => {
            reject(signal.reason);
          });
        });
      },
      model: "probe-model",
      log: { info: () => {}, error: () => {} },
    })(vote, closing.signal);
    closing.abort();

    // Raced against a deadline of its own: a probe that ignores the close
    // would otherwise run to its clock and fail as a bare runner timeout.
    const settled = await Promise.race([
      run.then(
        () => "resolved",
        () => "rejected",
      ),
      new Promise<string>((resolve) => {
        setTimeout(() => {
          resolve("still running after the close");
        }, CLOSE_DEADLINE_MS);
      }),
    ]);
    expect(
      settled,
      "an aborted probe ends at once, without a rejection",
    ).toBe("resolved");
    expect(
      upstreamSignal?.aborted,
      "closing the app aborts the upstream call of the probe in flight",
    ).toBe(true);
  });
});

describe("breaker probe — one event per probe", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("names the attempt and the outcome once per probe", async () => {
    const { createCircuitBreaker } = await import(
      "../../src/reliability/circuit-breaker.js"
    );
    const { createProbeRunner } = await import(
      "../../src/reliability/breaker-probe.js"
    );
    const captured = capturingLog();
    const responses: StreamingAdapter[] = [
      { kind: "upstream_error", status: 503, body_raw: CANARY },
      {
        kind: "accepted_stream",
        status: 200,
        reader: new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
            controller.close();
          },
        }).getReader(),
      },
    ];
    let call = 0;
    const breaker = createCircuitBreaker(
      captured.log,
      createProbeRunner({
        streaming: async () => {
          const response = responses[call];
          call += 1;
          if (response === undefined) throw new Error("no response scripted");
          return response;
        },
        model: "probe-model",
        log: captured.log,
      }),
    );
    for (let i = 0; i < 5; i++) breaker.recordResult("FAILURE");

    await vi.advanceTimersByTimeAsync(OPEN_MS);
    await vi.advanceTimersByTimeAsync(OPEN_MS);

    expect(
      captured.lines.filter((line) => line.event === "breaker_probe_result"),
      "each probe is one line: which attempt, how it ended, nothing from the upstream",
    ).toEqual([
      { event: "breaker_probe_result", attempt: 1, outcome: "FAILURE" },
      { event: "breaker_probe_result", attempt: 2, outcome: "SUCCESS" },
    ]);

    const runnerFailureLog = capturingLog();
    const runnerFailureBreaker = createCircuitBreaker(
      runnerFailureLog.log,
      () => {
        throw new Error("runner failed");
      },
    );
    for (let i = 0; i < 5; i++) {
      runnerFailureBreaker.recordResult("FAILURE");
    }
    await vi.advanceTimersByTimeAsync(OPEN_MS);

    expect(
      runnerFailureLog.lines.filter(
        (line) => line.event === "breaker_probe_result",
      ),
      "a breaker-generated FAILURE uses the same vote event",
    ).toEqual([
      { event: "breaker_probe_result", attempt: 1, outcome: "FAILURE" },
    ]);
  });
});
