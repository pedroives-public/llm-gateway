import { describe, it, expect } from "vitest";
import { randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { buildApp } from "../../src/app.js";
import {
  createAdmissionGate,
  type AdmissionGate,
} from "../../src/reliability/admission-gate.js";
import { fakeAuthDb, bearer } from "../helpers/fake-auth-db.js";
import { deferredAuthDb } from "../helpers/deferred-auth-db.js";

// INV-SLOT: no admission slot may be held without a release guaranteed at
// the moment it is acquired. Observable: after any terminal, the gate admits
// the next request. These cells drive the two terminations Fastify's hooks
// cannot see, each on a REAL socket (`inject` cannot kill one):
//   - the client socket closes after the body was read and while the
//     response is pending: `req.aborted` stays false (body complete) and
//     `finish` never comes, so neither onResponse nor onRequestAbort fires;
//   - the socket closes while auth is parked: onRequestAbort returns the
//     PRE slot, but the onRequest chain continues and acquires a POST slot
//     for the dead request — no future event will ever fire for it.
// Each cell races the release against a deadline whose message names the
// invariant and the door, so a violation reads as INV-SLOT in CI, never as
// a runner timeout.

// Deadline for the slot to come back after the socket dies. The release is
// event-driven (socket close → listener) or synchronous (check at
// acquisition), so tens of ms suffice; 2500 ms leaves the named RED room to
// win under vitest's 5000 ms default.
const RELEASE_DEADLINE_MS = 2500;

// Requests carrying this header park in the handler (the "awaiting the
// upstream" phase); the oracle request omits it and completes at once.
const PARK_HEADER = "x-park";

interface ObservedGate {
  gate: AdmissionGate;
  // Settles when the gate's single slot is returned.
  released: Promise<void>;
}

// Capacity-1 gate whose slot reports its own return; with one slot, the
// first release observed is the probe request's.
function observedGate(): ObservedGate {
  const inner = createAdmissionGate(1);
  let signal: () => void = () => {};
  const released = new Promise<void>((resolve) => {
    signal = resolve;
  });
  const gate: AdmissionGate = {
    tryAcquire() {
      const release = inner.tryAcquire();
      if (release === null) {
        return null;
      }
      return () => {
        release();
        signal();
      };
    },
  };
  return { gate, released };
}

async function releaseOrViolation(
  released: Promise<void>,
  violation: string,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(violation)), RELEASE_DEADLINE_MS);
  });
  try {
    await Promise.race([released, deadline]);
  } finally {
    clearTimeout(timer);
  }
}

// A complete POST with a JSON body over a real socket. Fastify's JSON parser
// consumes the body before the handler runs, which is what makes a later
// socket close invisible to the abort hook.
function openProbeSocket(
  port: number,
  headers: Record<string, string>,
): http.ClientRequest {
  const body = "{}";
  const client = http.request({
    host: "127.0.0.1",
    port,
    method: "POST",
    path: "/probe",
    headers: {
      ...headers,
      "content-type": "application/json",
      "content-length": String(Buffer.byteLength(body)),
    },
  });
  // destroy() surfaces on the client as ECONNRESET / socket hang up.
  client.on("error", () => {});
  client.end(body);
  return client;
}

describe("admission wiring — slot release on socket death (INV-SLOT)", () => {
  it("returns the POST_AUTH slot when the socket closes after the body was read", async () => {
    const fakeDb = fakeAuthDb(randomUUID());
    const post = observedGate();

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });
    let signalParked!: () => void;
    const parkedSignal = new Promise<void>((resolve) => {
      signalParked = resolve;
    });
    let abortHookFired = false;

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePost: post.gate,
      registerProtected: async (scope) => {
        // Fastify requires the async onRequestAbort hook to declare exactly
        // one parameter (checked at registration time). Only the parked
        // probe request counts: its socket is the one being killed.
        scope.addHook("onRequestAbort", async (request) => {
          if (request.headers[PARK_HEADER] === "1") {
            abortHookFired = true;
          }
        });
        scope.post("/probe", async (request) => {
          if (request.headers[PARK_HEADER] === "1") {
            signalParked();
            await parked;
          }
          return { ok: true };
        });
      },
    });

    try {
      await app.listen({ port: 0, host: "127.0.0.1" });
      const { port } = app.server.address() as AddressInfo;
      const client = openProbeSocket(port, {
        authorization: bearer(),
        [PARK_HEADER]: "1",
      });

      // Handler entry proves the body was consumed: from here on, closing
      // the socket leaves req.aborted false and the response unfinished.
      await parkedSignal;
      client.destroy();

      await releaseOrViolation(
        post.released,
        "INV-SLOT violated (door: socket closed after the body was read, " +
          "response pending) — the POST_AUTH slot was never returned. This " +
          "termination fires neither onResponse nor onRequestAbort, so a " +
          "release not guaranteed at acquisition is lost",
      );

      // Door fidelity: this exit must stay invisible to onRequestAbort. If
      // it fired, the body was not fully read and the cell drifted onto the
      // door the mid-flight abort test already covers.
      expect(abortHookFired).toBe(false);

      // Behavioral oracle: with a single POST_AUTH slot, the next request is
      // admitted only if the dead request's slot really came back.
      const next = await app.inject({
        method: "POST",
        url: "/probe",
        headers: { authorization: bearer() },
        payload: {},
      });
      expect(next.statusCode).toBe(200);
    } finally {
      open();
      await app.close();
    }
  });

  it("returns a POST_AUTH slot acquired after the socket had already closed", async () => {
    const deferred = deferredAuthDb([randomUUID(), randomUUID()]);
    const post = observedGate();

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });
    let signalAbort!: () => void;
    const abortSeen = new Promise<void>((resolve) => {
      signalAbort = resolve;
    });

    const app = await buildApp({
      logger: false,
      db: deferred.db,
      admissionGatePost: post.gate,
      registerProtected: async (scope) => {
        scope.addHook("onRequestAbort", async (request) => {
          if (request.headers[PARK_HEADER] === "1") {
            signalAbort();
          }
        });
        scope.post("/probe", async (request) => {
          if (request.headers[PARK_HEADER] === "1") {
            await parked;
          }
          return { ok: true };
        });
      },
    });

    try {
      await app.listen({ port: 0, host: "127.0.0.1" });
      const { port } = app.server.address() as AddressInfo;
      const client = openProbeSocket(port, {
        authorization: bearer(),
        [PARK_HEADER]: "1",
      });

      // Parked at the auth seam: PRE slot held, body still unread.
      const held = await deferred.reached(1);
      client.destroy();

      // Door fidelity and ordering in one wait: onRequestAbort fires only
      // for an incomplete request, so seeing it proves the body was unread;
      // awaiting it before resuming auth makes "the socket died BEFORE the
      // POST acquisition" enforced, not assumed.
      await abortSeen;
      held.release();

      await releaseOrViolation(
        post.released,
        "INV-SLOT violated (door: socket closed during auth, POST_AUTH slot " +
          "acquired afterwards) — the slot acquired for an already-dead " +
          "request was never returned. The close edge had passed before " +
          "acquisition; only a synchronous dead-socket check at acquisition " +
          "can return it",
      );

      // Behavioral oracle: single POST_AUTH slot, next request admitted.
      const next = app.inject({
        method: "POST",
        url: "/probe",
        headers: { authorization: bearer() },
        payload: {},
      });
      const held2 = await deferred.reached(2);
      held2.release();
      expect((await next).statusCode).toBe(200);
    } finally {
      open();
      await app.close();
    }
  });
});
