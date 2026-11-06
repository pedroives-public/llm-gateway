import http from "node:http";
import { randomUUID } from "node:crypto";
import type { FastifyInstance, LightMyRequestResponse } from "fastify";
import { vi } from "vitest";
import { buildApp } from "../../src/app.js";
import { listenEphemeral } from "./ephemeral-server.js";
import { bearer, fakeAuthDb } from "./fake-auth-db.js";

// The real app builds its own breaker with the module's fixed thresholds.
// These mirror FAILURE_LIMIT and OPEN_COOLDOWN_MS in
// src/reliability/circuit-breaker.ts; if either changes there, openBreaker or
// elapseCooldown fails with a sentence saying the scene no longer fits.
const BREAKER_FAILURE_LIMIT = 5;
const BREAKER_OPEN_COOLDOWN_MS = 60_000;
// Real-time bound for the probe to reach the local upstream once the cooldown
// fires. Measured at 23 ms on 2026-11-06; 2 s leaves room for a slow runner
// and stays under Vitest's 5 s test timeout, so a missing probe fails here,
// with its own sentence, and not as a bare test timeout.
const PROBE_ARRIVAL_DEADLINE_MS = 2_000;
// Real-time polling step while waiting for the probe to arrive.
const POLL_STEP_MS = 10;

// Captured before any test installs fake timers, so deadlines run on real time.
const realSetTimeout = globalThis.setTimeout;

export type CapturedRequest = {
  method: string;
  url: string;
  headers: http.IncomingHttpHeaders;
  body: string;
};

export type TenantRequest = {
  headers?: Record<string, string>;
  payload?: unknown;
};

export interface RealAppScene {
  app: FastifyInstance;
  // Requests the local upstream received, split by origin. With the
  // streaming flag off (the real app's default in tests), the route rejects
  // stream: true before the upstream, so only the probe sends it.
  tenantRequests: () => readonly CapturedRequest[];
  probeRequests: () => readonly CapturedRequest[];
  send: (request?: TenantRequest) => Promise<LightMyRequestResponse>;
  // Sends BREAKER_FAILURE_LIMIT tenant requests, each answered 500 by the
  // upstream, which opens the breaker.
  openBreaker: (request?: TenantRequest) => Promise<void>;
  // Advances the fake clock past the cooldown and waits, in real time, until
  // the probe reaches the upstream. The upstream holds the probe, so the
  // breaker stays HALF_OPEN until the scene closes.
  elapseCooldown: () => Promise<void>;
  close: () => Promise<void>;
}

const DEFAULT_PAYLOAD = {
  model: "gpt-4o-mini",
  messages: [{ role: "user", content: "hi" }],
};

// The real app (real breaker, real probe runner, real adapter) against a
// local upstream that answers tenants 500 and holds the probe. Only
// setTimeout and clearTimeout are faked: the breaker's cooldown then moves on
// demand while the inbound inject and the outbound sockets stay real.
export async function startRealAppScene(): Promise<RealAppScene> {
  let tenants: readonly CapturedRequest[] = [];
  let probes: readonly CapturedRequest[] = [];

  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk: Buffer) => {
      body += chunk.toString("utf8");
    });
    req.on("end", () => {
      const captured: CapturedRequest = {
        method: req.method ?? "",
        url: req.url ?? "",
        headers: req.headers,
        body,
      };
      const parsed = JSON.parse(body) as { stream?: unknown };
      if (parsed.stream === true) {
        probes = [...probes, captured];
        return;
      }
      tenants = [...tenants, captured];
      res.writeHead(500, { "content-type": "application/json" });
      res.end(
        JSON.stringify({ error: { message: "upstream failure", type: "server_error" } }),
      );
    });
  });
  const upstream = await listenEphemeral(server);

  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const previousBaseUrl = process.env["OPENAI_BASE_URL"];
  let app: FastifyInstance;
  try {
    // The real app reads its upstream endpoint from the environment at build
    // time; this is its only seam for the endpoint.
    process.env["OPENAI_BASE_URL"] = `http://127.0.0.1:${upstream.port}/v1`;
    app = await buildApp({ logger: false, db: fakeAuthDb(randomUUID()) });
  } catch (error) {
    vi.useRealTimers();
    server.closeAllConnections();
    await upstream.close();
    throw error;
  } finally {
    if (previousBaseUrl === undefined) {
      delete process.env["OPENAI_BASE_URL"];
    } else {
      process.env["OPENAI_BASE_URL"] = previousBaseUrl;
    }
  }

  const send = (request: TenantRequest = {}) =>
    app.inject({
      method: "POST",
      url: "/v1/chat/completions",
      headers: {
        authorization: bearer(),
        "content-type": "application/json",
        ...request.headers,
      },
      payload: request.payload ?? DEFAULT_PAYLOAD,
    });

  return {
    app,
    tenantRequests: () => tenants,
    probeRequests: () => probes,
    send,
    openBreaker: async (request) => {
      for (let i = 0; i < BREAKER_FAILURE_LIMIT; i += 1) {
        const response = await send(request);
        if (response.statusCode !== 502) {
          throw new Error(
            `opening the breaker needs ${BREAKER_FAILURE_LIMIT} upstream failures; request ${i + 1} got ${response.statusCode}`,
          );
        }
      }
    },
    elapseCooldown: async () => {
      await vi.advanceTimersByTimeAsync(BREAKER_OPEN_COOLDOWN_MS);
      const startedAt = performance.now();
      while (probes.length === 0) {
        if (performance.now() - startedAt > PROBE_ARRIVAL_DEADLINE_MS) {
          throw new Error(
            `the cooldown elapsed but no probe reached the upstream within ${PROBE_ARRIVAL_DEADLINE_MS} ms`,
          );
        }
        await new Promise((resolve) => realSetTimeout(resolve, POLL_STEP_MS));
      }
    },
    close: async () => {
      try {
        await app.close();
      } finally {
        vi.useRealTimers();
        server.closeAllConnections();
        await upstream.close();
      }
    },
  };
}
