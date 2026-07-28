import { afterEach, describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../../src/app.js";
import { proxyRoute } from "../../src/routes/proxy.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { deferredAuthDb } from "../helpers/deferred-auth-db.js";
import { makeLogCapture } from "../log-capture.js";

// Tenant isolation under concurrency: two requests in flight, each keeping the
// identity its own auth lookup returned. authPreHandler writes that identity
// after an awaited database lookup, and the await yields the event loop — so a
// second request can authenticate inside the first one's suspension, and a
// shared slot would swap them there. In V1 tenantId reaches no query, quota or
// storage, only structured events, so a swap surfaces as a misattributed event.

const TENANT_A = "11111111-1111-4111-8111-111111111111";
const TENANT_B = "22222222-2222-4222-8222-222222222222";

// Correlation label: req_id never leaves the process, so the test cannot use it
// to tell the two req_start events apart. Idempotency-Key is client-controlled,
// republished as idempotency_key_present, and reaches the event via the HTTP
// header parser — a different mechanism from the decorated tenantId, so one
// defect cannot corrupt both and leave the pair looking coherent.
const LABEL_A = "probe-a";

const body = {
  model: "llama-3.1-8b-instant",
  messages: [{ role: "user", content: "probe" }],
};

// One-shot barrier: `reached` settles on arrival, `open` releases. Two of them
// spell out the ordering the leak requires.
function gate(): {
  reached: Promise<void>;
  arrive: () => void;
  open: () => void;
  passed: Promise<void>;
} {
  let arrive: () => void = () => {};
  let open: () => void = () => {};
  const reached = new Promise<void>((resolve) => {
    arrive = resolve;
  });
  const passed = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { reached, arrive, open, passed };
}

let app: FastifyInstance | undefined;

afterEach(async () => {
  await app?.close();
  app = undefined;
});

describe("A-1 tenant isolation under concurrent requests", () => {
  it("each request keeps the tenant its own auth lookup returned", async () => {
    const capture = makeLogCapture();
    const auth = deferredAuthDb([TENANT_A, TENANT_B]);

    // The parking point the leak needs: between auth's write and the handler's
    // read of tenantId. In production that stretch is body parsing and
    // validation, async and yielding; here a preHandler hook holds only the
    // labelled request, so the other request's write lands inside the window.
    const afterWrite = gate();

    app = await buildApp({
      logger: capture.logger,
      db: auth.db,
      registerProtected: async (scope) => {
        scope.addHook("preHandler", async (request) => {
          if (request.headers["idempotency-key"] === LABEL_A) {
            afterWrite.arrive();
            await afterWrite.passed;
          }
        });
        await scope.register(proxyRoute, {
          breaker: stubBreaker,
          upstreamBuffered: (): Promise<Outcome> =>
            Promise.resolve({ kind: "ok", status: 200, body_parsed: {} }),
        });
      },
    });

    // The leak is an ordering of three events, not two:
    //   write(A) → write(B) → read(A)
    // Each arrow below is enforced by an await, never by scheduling luck.
    // Parking A before its own write serialises the pairs and hides the leak.

    // Arrow 1 — write(A), then park after the write.
    const responseA = app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer(), "idempotency-key": LABEL_A },
      payload: body,
    });
    (await auth.reached(1)).release();
    await afterWrite.reached;

    // Arrow 2 — write(B), inside A's window; B then reads and finishes.
    const responseB = app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: { authorization: bearer() },
      payload: body,
    });
    (await auth.reached(2)).release();
    await responseB;

    // Arrow 3 — read(A). Only now does A reach the handler.
    afterWrite.open();
    await responseA;

    const started = capture.byEvent("req_start");
    expect(started).toHaveLength(2);

    const eventA = started.find(
      (event) => event.idempotency_key_present === true,
    );

    const eventB = started.find(
      (event) => event.idempotency_key_present === false,
    );

    expect(eventA?.tenant_id).toBe(TENANT_A);
    expect(eventB?.tenant_id).toBe(TENANT_B);
  });
});
