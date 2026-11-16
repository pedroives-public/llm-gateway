import http from "node:http";
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { PlanTier } from "../../src/db/schema.js";
import type { CircuitBreaker } from "../../src/reliability/circuit-breaker.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { bearer, fakeAuthDb } from "../helpers/fake-auth-db.js";
import { buildProxyApp } from "../helpers/proxy-app.js";
import { makeLogCapture, type CapturedLog } from "../log-capture.js";

// Model allowlist per plan: a request whose model is not on the list of the
// tenant's plan is refused with 403 before the circuit breaker is consulted
// and before any upstream call. Each plan contains the plan below it, and each
// list is ordered by model family, oldest first:
//
//   free        gpt-4o-mini
//   pro         gpt-4o-mini, gpt-5-mini, gpt-5.6-luna
//   enterprise  gpt-4o-mini, gpt-4.1, gpt-5-mini, gpt-5.1, gpt-5.2, gpt-5.6-luna
//
// Matching is exact string equality, case included. The checks run in this
// order: authentication (401), body validation (400), allowlist (403), circuit
// breaker (503).
//
// Every scene runs the real route and the real adapter against a local server
// that counts the requests it receives, so "the upstream was not called" is a
// count on a socket, not a spy on a function.

type Scene = {
  plan: PlanTier;
  // An object is sent as JSON. A string is sent as written.
  body: Record<string, unknown> | string;
  // `null` sends no Authorization header.
  authorization?: string | null;
  breakerFastFails?: boolean;
};

type SceneResult = {
  status: number;
  errorClass: string | undefined;
  contentType: string;
  payload: string;
  upstreamRequests: number;
  breakerAcquires: number;
  breakerVotes: number;
  tenantId: string;
  logs: CapturedLog[];
  byEvent: (event: string) => CapturedLog[];
};

async function runScene(scene: Scene): Promise<SceneResult> {
  // Fake upstream: counts every request and answers the way a provider
  // answers that path, so an allowed request completes.
  let upstreamRequests = 0;
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => {
      chunks.push(chunk);
    });
    req.on("end", () => {
      upstreamRequests += 1;
      const text = Buffer.concat(chunks).toString("utf8");
      if (text.includes('"stream":true')) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end("data: [DONE]\n\n");
      } else {
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"id":"chatcmpl-fence"}');
      }
    });
  });
  const { port, close } = await listenEphemeral(server);

  let breakerAcquires = 0;
  let breakerVotes = 0;
  const breaker: CircuitBreaker = {
    tryAcquire: () => {
      breakerAcquires += 1;
      return { kind: scene.breakerFastFails === true ? "FAST_FAIL" : "NORMAL" };
    },
    recordResult: () => {
      breakerVotes += 1;
    },
    getState: () => "CLOSED",
    close: () => {},
  };

  const client = createOpenAIClient({
    apiKey: "deployment-key",
    baseURL: `http://127.0.0.1:${port}`,
  });
  const capture = makeLogCapture();
  const tenantId = randomUUID();
  const app = await buildProxyApp({
    breaker,
    streamingEnabled: true,
    upstreamBuffered: client.buffered,
    upstreamStreaming: client.streaming,
    db: fakeAuthDb(tenantId, scene.plan),
    logger: capture.logger,
  });

  const authorization =
    scene.authorization === undefined ? bearer() : scene.authorization;

  try {
    const res = await app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: {
        ...(authorization === null ? {} : { authorization }),
        "content-type": "application/json",
      },
      payload:
        typeof scene.body === "string"
          ? scene.body
          : JSON.stringify(scene.body),
    });

    const errorClass = res.headers["x-gateway-error-class"];
    return {
      status: res.statusCode,
      errorClass: typeof errorClass === "string" ? errorClass : undefined,
      contentType: String(res.headers["content-type"] ?? ""),
      payload: res.payload,
      upstreamRequests,
      breakerAcquires,
      breakerVotes,
      tenantId,
      logs: capture.logs,
      byEvent: capture.byEvent,
    };
  } finally {
    await app.close();
    await close();
  }
}

const MESSAGES = [{ role: "user", content: "hi" }];

type AllowedRow = { plan: PlanTier; model: string };

const ALLOWED_ROWS: AllowedRow[] = [
  { plan: "free", model: "gpt-4o-mini" },
  { plan: "pro", model: "gpt-4o-mini" },
  { plan: "pro", model: "gpt-5-mini" },
  { plan: "pro", model: "gpt-5.6-luna" },
  { plan: "enterprise", model: "gpt-4o-mini" },
  { plan: "enterprise", model: "gpt-4.1" },
  { plan: "enterprise", model: "gpt-5-mini" },
  { plan: "enterprise", model: "gpt-5.1" },
  { plan: "enterprise", model: "gpt-5.2" },
  { plan: "enterprise", model: "gpt-5.6-luna" },
];

type RefusedRow = {
  name: string;
  plan: PlanTier;
  model: string;
  message: string;
};

const REFUSED_ROWS: RefusedRow[] = [
  {
    name: "free asks for gpt-5-mini",
    plan: "free",
    model: "gpt-5-mini",
    message:
      "gateway: model 'gpt-5-mini' is not allowed for tier 'free'. Allowed: gpt-4o-mini",
  },
  {
    name: "free asks for gpt-4.1",
    plan: "free",
    model: "gpt-4.1",
    message:
      "gateway: model 'gpt-4.1' is not allowed for tier 'free'. Allowed: gpt-4o-mini",
  },
  {
    name: "pro asks for gpt-4.1",
    plan: "pro",
    model: "gpt-4.1",
    message:
      "gateway: model 'gpt-4.1' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "pro asks for gpt-5.1",
    plan: "pro",
    model: "gpt-5.1",
    message:
      "gateway: model 'gpt-5.1' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "pro asks for gpt-5.2",
    plan: "pro",
    model: "gpt-5.2",
    message:
      "gateway: model 'gpt-5.2' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "enterprise asks for gpt-6.1-sol",
    plan: "enterprise",
    model: "gpt-6.1-sol",
    message:
      "gateway: model 'gpt-6.1-sol' is not allowed for tier 'enterprise'. Allowed: gpt-4o-mini, gpt-4.1, gpt-5-mini, gpt-5.1, gpt-5.2, gpt-5.6-luna",
  },
  {
    name: "exact match: a dated name is not the listed name",
    plan: "pro",
    model: "gpt-4o-mini-2024-07-18",
    message:
      "gateway: model 'gpt-4o-mini-2024-07-18' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "exact match: a different case is not the listed name",
    plan: "pro",
    model: "GPT-4o-mini",
    message:
      "gateway: model 'GPT-4o-mini' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "exact match: a trailing space is not the listed name",
    plan: "pro",
    model: "gpt-5-mini ",
    message:
      "gateway: model 'gpt-5-mini ' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  // The next four look like a listed name on a screen and are a different
  // text: the comparison takes the text as sent, with no normalization and
  // nothing stripped.
  {
    name: "exact match: fullwidth letters are not the listed name",
    plan: "pro",
    model: "\uff47\uff50\uff54\uff0d\uff14\uff4f\uff0d\uff4d\uff49\uff4e\uff49",
    message:
      "gateway: model '\uff47\uff50\uff54\uff0d\uff14\uff4f\uff0d\uff4d\uff49\uff4e\uff49' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "exact match: a trailing zero-width space is not the listed name",
    plan: "pro",
    model: "gpt-4o-mini\u200b",
    message:
      "gateway: model 'gpt-4o-mini\u200b' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "exact match: a trailing NUL is not the listed name",
    plan: "pro",
    model: "gpt-4o-mini\u0000",
    message:
      "gateway: model 'gpt-4o-mini\u0000' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
  {
    name: "exact match: a leading byte-order mark is not the listed name",
    plan: "pro",
    model: "\ufeffgpt-4o-mini",
    message:
      "gateway: model '\ufeffgpt-4o-mini' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
  },
];

describe("model allowlist per plan", () => {
  it.each(ALLOWED_ROWS)(
    "$plan asks for $model: forwarded",
    async ({ plan, model }) => {
      const scene = await runScene({
        plan,
        body: { model, messages: MESSAGES },
      });

      expect(
        scene.status,
        "a model on the plan's list is answered 200",
      ).toBe(200);
      expect(
        scene.upstreamRequests,
        "a model on the plan's list reaches the upstream exactly once",
      ).toBe(1);
      expect(
        scene.byEvent("req_rejected"),
        "an allowed request emits no req_rejected",
      ).toHaveLength(0);
    },
  );

  it.each(REFUSED_ROWS)("$name: refused", async ({ plan, model, message }) => {
    const scene = await runScene({
      plan,
      body: { model, messages: MESSAGES },
    });

    expect(
      scene.status,
      "a model outside the plan's list is refused with 403",
    ).toBe(403);
    expect(
      scene.errorClass,
      "the refusal carries the client-fault class",
    ).toBe("client-fault");
    expect(
      JSON.parse(scene.payload),
      "the refusal names the model, the plan and the plan's list, with type permission_error and code model_not_allowed",
    ).toEqual({
      error: {
        message,
        type: "permission_error",
        code: "model_not_allowed",
      },
    });
    expect(
      scene.upstreamRequests,
      "a refused request never reaches the upstream",
    ).toBe(0);
    expect(
      scene.breakerAcquires,
      "a refused request does not consult the circuit breaker",
    ).toBe(0);
    expect(
      scene.breakerVotes,
      "a refused request records no vote on the circuit breaker",
    ).toBe(0);
  });

  it("streaming: an allowed model streams, a refused one gets the JSON refusal and no event stream", async () => {
    const allowed = await runScene({
      plan: "pro",
      body: { model: "gpt-5-mini", messages: MESSAGES, stream: true },
    });
    expect(
      allowed.status,
      "control: an allowed model on a streaming request is answered 200",
    ).toBe(200);
    expect(
      allowed.contentType,
      "control: an allowed streaming request is answered as an event stream",
    ).toContain("text/event-stream");
    expect(
      allowed.upstreamRequests,
      "control: an allowed streaming request reaches the upstream exactly once",
    ).toBe(1);

    const refused = await runScene({
      plan: "pro",
      body: { model: "gpt-4.1", messages: MESSAGES, stream: true },
    });
    expect(
      refused.status,
      "a model outside the plan's list is refused with 403 on a streaming request too",
    ).toBe(403);
    expect(
      refused.contentType,
      "the refusal of a streaming request is a JSON answer, never an event stream",
    ).toContain("application/json");
    expect(
      JSON.parse(refused.payload),
      "the refusal of a streaming request carries the same body as any other refusal",
    ).toEqual({
      error: {
        message:
          "gateway: model 'gpt-4.1' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna",
        type: "permission_error",
        code: "model_not_allowed",
      },
    });
    expect(
      refused.upstreamRequests,
      "a refused streaming request never reaches the upstream",
    ).toBe(0);
  });

  it("order: authentication answers before the allowlist", async () => {
    const scene = await runScene({
      plan: "pro",
      authorization: null,
      body: { model: "gpt-4.1", messages: MESSAGES },
    });

    expect(
      scene.status,
      "a request with no key is answered 401 whatever model it asks for",
    ).toBe(401);
    expect(JSON.parse(scene.payload)).toEqual({ error: "Unauthorized" });
    expect(scene.upstreamRequests).toBe(0);
  });

  it("order: body validation answers before the allowlist", async () => {
    const scene = await runScene({
      plan: "pro",
      body: { model: "gpt-4.1", messages: MESSAGES, max_tokens: 16385 },
    });

    expect(
      scene.status,
      "a body that fails validation is answered 400 whatever model it asks for",
    ).toBe(400);
    expect(
      (JSON.parse(scene.payload) as { error: { code: string } }).error.code,
      "the 400 names the body rule that failed, not the allowlist",
    ).toBe("max_tokens_too_large");
    expect(scene.upstreamRequests).toBe(0);
  });

  it("order: the allowlist answers before the circuit breaker is consulted", async () => {
    // Control: this breaker does fast-fail a request the allowlist lets
    // through, so the 403 below is the allowlist answering first and not a
    // breaker that never refuses.
    const control = await runScene({
      plan: "pro",
      breakerFastFails: true,
      body: { model: "gpt-4o-mini", messages: MESSAGES },
    });
    expect(
      control.status,
      "control: with this breaker an allowed model is answered 503",
    ).toBe(503);
    expect(
      control.breakerAcquires,
      "control: an allowed request consults the circuit breaker once",
    ).toBe(1);

    const scene = await runScene({
      plan: "pro",
      breakerFastFails: true,
      body: { model: "gpt-4.1", messages: MESSAGES },
    });
    expect(
      scene.status,
      "a model outside the plan's list is refused with 403 even when the circuit breaker would fast-fail",
    ).toBe(403);
    expect(
      scene.breakerAcquires,
      "a refused request does not consult the circuit breaker",
    ).toBe(0);
  });

  it("length: a model longer than 256 characters is refused by body validation, so the refusal message has a bound", async () => {
    // The allowlist's refusal repeats the requested model. Without a limit on
    // its length, the answer grows with whatever text a client sends.
    const LIMIT = 256;

    const atLimit = "m".repeat(LIMIT);
    const allowed = await runScene({
      plan: "pro",
      body: { model: atLimit, messages: MESSAGES },
    });
    expect(
      allowed.status,
      "control: a model of exactly 256 characters passes body validation and gets the allowlist's refusal",
    ).toBe(403);
    expect(
      (JSON.parse(allowed.payload) as { error: { message: string } }).error
        .message,
      "control: a model of exactly 256 characters is repeated whole in the refusal",
    ).toBe(
      `gateway: model '${atLimit}' is not allowed for tier 'pro'. Allowed: gpt-4o-mini, gpt-5-mini, gpt-5.6-luna`,
    );

    // The largest refusal there can be: every character needs escaping, on
    // the plan with the longest list.
    const worstCase = await runScene({
      plan: "enterprise",
      body: { model: "\u0000".repeat(LIMIT), messages: MESSAGES },
    });
    expect(
      worstCase.status,
      "control: 256 characters that need escaping pass body validation and get the allowlist's refusal",
    ).toBe(403);
    expect(
      Buffer.byteLength(worstCase.payload),
      "the largest refusal stays under 2048 bytes",
    ).toBeLessThan(2048);

    for (const length of [LIMIT + 1, 200_000]) {
      const scene = await runScene({
        plan: "pro",
        body: { model: "m".repeat(length), messages: MESSAGES },
      });
      expect(
        scene.status,
        `a model of ${length} characters is refused by body validation with 400`,
      ).toBe(400);
      expect(
        scene.errorClass,
        "the body-validation refusal carries the client-fault class",
      ).toBe("client-fault");
      expect(
        Buffer.byteLength(scene.payload),
        `the answer to a model of ${length} characters stays under 512 bytes`,
      ).toBeLessThan(512);
      expect(
        scene.upstreamRequests,
        "a model longer than the limit never reaches the upstream",
      ).toBe(0);
      expect(
        scene.breakerAcquires,
        "a model longer than the limit does not consult the circuit breaker",
      ).toBe(0);
      expect(
        scene.byEvent("req_rejected").map((event) => event.reason),
        "a model longer than the limit is rejected as a schema violation, not by the allowlist",
      ).toEqual(["schema_validation"]);
    }
  });

  it("log: one req_rejected, no req_start or req_complete, and the requested model in no log line", async () => {
    // A model name no list holds and no other test uses, so finding it in a
    // log line can only mean the request's model was written there.
    const CANARY_MODEL = "fence-canary-7c1e";
    const scene = await runScene({
      plan: "pro",
      body: { model: CANARY_MODEL, messages: MESSAGES },
    });

    expect(
      scene.status,
      "a model outside the plan's list is refused with 403",
    ).toBe(403);
    expect(
      scene.payload,
      "control: the canary is what the request asked for, and the refusal message repeats it to the client",
    ).toContain(CANARY_MODEL);

    const rejected = scene.byEvent("req_rejected");
    expect(
      rejected,
      "a refused request emits exactly one req_rejected",
    ).toHaveLength(1);
    const event = rejected[0] ?? {};
    expect(
      event,
      "the req_rejected of a refusal carries reason model_not_allowed and status 403",
    ).toMatchObject({
      event: "req_rejected",
      tenant_id: scene.tenantId,
      route: "/v1/chat/completions",
      reason: "model_not_allowed",
      status: 403,
    });

    // Allowlist of fields: the contract's own plus the logger's.
    const LOGGER_KEYS = ["level", "time", "pid", "hostname", "reqId"];
    const CONTRACT_KEYS = [
      "event",
      "req_id",
      "tenant_id",
      "route",
      "reason",
      "status",
    ];
    expect(
      Object.keys(event).filter(
        (key) => !LOGGER_KEYS.includes(key) && !CONTRACT_KEYS.includes(key),
      ),
      "the req_rejected of a refusal carries no field beyond the contract's",
    ).toEqual([]);

    expect(
      scene.byEvent("req_start"),
      "a refused request emits no req_start",
    ).toHaveLength(0);
    expect(
      scene.byEvent("req_complete"),
      "a refused request emits no req_complete",
    ).toHaveLength(0);

    // Explicit net, on top of the field allowlist above: the model is client
    // input and no log line of any kind may carry it.
    expect(
      scene.logs.filter((line) => JSON.stringify(line).includes(CANARY_MODEL)),
      "the requested model appears in no log line",
    ).toEqual([]);
  });
});
