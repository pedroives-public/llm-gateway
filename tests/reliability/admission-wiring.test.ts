import { describe, it, expect } from "vitest";
import { randomBytes, randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import type { AdmissionGate } from "../../src/reliability/admission-gate.js";
import { buildApp } from "../../src/app.js";
import { fakeAuthDb, bearer } from "../helpers/fake-auth-db.js";
import { deferredAuthDb } from "../helpers/deferred-auth-db.js";
import { createAdmissionGate } from "../../src/reliability/admission-gate.js";

// Integration cells for the admission gates wired around the protected scope.
// House pattern: buildApp({ registerProtected, admissionGatePre/Post }) with
// fakeAuthDb (instant auth) or deferredAuthDb (parks at the auth seam), a
// handler that parks on a deferred promise, and app.inject() fired WITHOUT
// await — await only the assertions. First describe: the POST_AUTH budget
// (tiny injected gate; the pin builds the DEFAULT app: 41 parked, 42nd ->
// 503). Second: the PRE_AUTH bulkhead — parked requests hold PRE slots and
// the handoff must return them.

describe("admission wiring — protected scope", () => {
  it("admits authenticated requests while slots are free", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;

    const fakeDb = fakeAuthDb(tenantId);

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePost: createAdmissionGate(2),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          return { ok: true };
        });
      },
    });

    try {
      const res = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });
      expect(res.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("refuses with 503 admission_full once the gate is full", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const fakeDb = fakeAuthDb(tenantId);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let parkedCount = 0;
    let bothParked!: () => void;
    const bothParkedSignal = new Promise<void>((resolve) => {
      bothParked = resolve;
    });

    let thirdEntered!: () => void;
    const thirdEnteredSignal = new Promise<void>((resolve) => {
      thirdEntered = resolve;
    });

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePost: createAdmissionGate(2),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          parkedCount++;
          if (parkedCount === 2) {
            bothParked();
          }
          if (parkedCount === 3) {
            thirdEntered();
          }
          await parked;
          return { ok: true };
        });
      },
    });

    try {
      const req1 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      const req2 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      // Wait until both requests are parked (in-flight)
      await bothParkedSignal;

      const req3 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      // Race the refusal against a named violation: if a third request ever
      // enters the handler, the gate admitted past capacity — fail fast with
      // the reason instead of hanging until the test timeout.
      const req3Result = await Promise.race([
        req3,
        thirdEnteredSignal.then((): never => {
          throw new Error(
            "a third request entered the handler while the gate was full",
          );
        }),
      ]);

      expect(req3Result.statusCode).toBe(503);
      const body = JSON.parse(req3Result.body);
      expect(body.error.type).toBe("service_unavailable");
      expect(body.error.code).toBe("admission_full");

      // Now release the parked requests
      open();
      const [res1, res2] = await Promise.all([req1, req2]);

      expect(res1.statusCode).toBe(200);
      expect(res2.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("a completed request frees its slot for the next one", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const fakeDb = fakeAuthDb(tenantId);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let parkedCount = 0;
    let bothParked!: () => void;
    const bothParkedSignal = new Promise<void>((resolve) => {
      bothParked = resolve;
    });

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePost: createAdmissionGate(2),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          parkedCount++;
          if (parkedCount === 2) {
            bothParked();
          }
          await parked;
          return { ok: true };
        });
      },
    });

    try {
      const req1 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      const req2 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      // Wait until both requests are parked (in-flight)
      await bothParkedSignal;

      // Release the parked requests
      open();
      const [res1, res2] = await Promise.all([req1, req2]);

      expect(res1.statusCode).toBe(200);
      expect(res2.statusCode).toBe(200);

      // Now a new request should succeed since a slot was freed
      const req3 = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      expect(req3.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("health stays reachable while the gate is full", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const fakeDb = fakeAuthDb(tenantId);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let parkedCount = 0;
    let bothParked!: () => void;
    const bothParkedSignal = new Promise<void>((resolve) => {
      bothParked = resolve;
    });

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePost: createAdmissionGate(2),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          parkedCount++;
          if (parkedCount === 2) {
            bothParked();
          }
          await parked;
          return { ok: true };
        });
      },
    });

    try {
      const req1 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      const req2 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      // Wait until both requests are parked (in-flight)
      await bothParkedSignal;

      const healthCheck = await app.inject({
        method: "GET",
        url: "/health",
      });

      expect(healthCheck.statusCode).toBe(200);

      // Now release the parked requests
      open();
      const [res1, res2] = await Promise.all([req1, req2]);

      expect(res1.statusCode).toBe(200);
      expect(res2.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("production default capacity is 41 — behavioral pin", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const fakeDb = fakeAuthDb(tenantId);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let notifyParked: (() => void) | null = null;

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          notifyParked?.();
          await parked;
          return { ok: true };
        });
      },
    });

    try {
      // Sequential arrivals: a concurrent burst of 41 would be refused by
      // the PRE gate (capacity 10) by design — this pin isolates the POST
      // budget.
      const requests = [];
      for (let i = 0; i < 41; i++) {
        const arrivedInHandler = new Promise<void>((resolve) => {
          notifyParked = resolve;
        });

        const request = app.inject({
          method: "GET",
          url: "/probe",
          headers: { authorization: `Bearer ${apiKey}` },
        });
        requests.push(request);

        // Fail fast with the reason: resolving instead of parking means a
        // refusal or early completion — capacity below 41 or a lost slot.
        const early = await Promise.race([
          arrivedInHandler.then(() => null),
          request.then((res) => res),
        ]);
        if (early !== null) {
          throw new Error(
            `request ${i + 1} resolved with ${early.statusCode} before ` +
              "parking — effective capacity is below 41 or a slot was never returned",
          );
        }
      }

      const extraReq = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      expect(extraReq.statusCode).toBe(503);
      const body = JSON.parse(extraReq.body);
      expect(body.error.type).toBe("service_unavailable");
      expect(body.error.code).toBe("admission_full");

      // Now release the parked requests
      open();
      const results = await Promise.all(requests);

      for (const res of results) {
        expect(res.statusCode).toBe(200);
      }
    } finally {
      open();
      await app.close();
    }
  });

  it("releases the slot when the client aborts mid-flight", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;
    const fakeDb = fakeAuthDb(tenantId);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let signalParked!: () => void;
    const parkedSignal = new Promise<void>((resolve) => {
      signalParked = resolve;
    });

    let signalReleased!: () => void;
    const releasedSignal = new Promise<void>((resolve) => {
      signalReleased = resolve;
    });

    const gate = createAdmissionGate(1);
    const spyGate: AdmissionGate = {
      tryAcquire() {
        const releaseSlot = gate.tryAcquire();
        if (releaseSlot === null) {
          return null;
        }

        return () => {
          releaseSlot();
          signalReleased();
        };
      },
    };

    let entries = 0;
    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePost: spyGate,
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          entries++;
          if (entries === 1) {
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
      const req = http.request({
        host: "127.0.0.1",
        port,
        path: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });
      req.on("error", () => {});
      req.end();

      await parkedSignal;
      req.destroy();

      let timer: ReturnType<typeof setTimeout> | undefined;
      const failSafe = new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error("the aborted request never released its slot"));
        }, 2500);
      });

      await Promise.race([releasedSignal, failSafe]);
      clearTimeout(timer);

      const secondRequest = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: `Bearer ${apiKey}` },
      });

      expect(secondRequest.statusCode).toBe(200);
    } finally {
      open();
      await app.close();
    }
  });
});

describe("admission wiring — pre-auth bulkhead", () => {
  it("refuses the next arrival once the pre-auth budget is full", async () => {
    const deferred = deferredAuthDb([randomUUID(), randomUUID()]);

    const app = await buildApp({
      logger: false,
      db: deferred.db,
      admissionGatePre: createAdmissionGate(2),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          return { ok: true };
        });
      },
    });

    try {
      // Each request parks at the auth seam, holding its PRE_AUTH slot.
      const req1 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      const held1 = await deferred.reached(1);

      const req2 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      const held2 = await deferred.reached(2);

      // Must be refused BEFORE the auth lookup — deferredAuthDb throws its
      // own sentence on an undeclared lookup, so overcapacity fails named.
      const req3 = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });

      expect(req3.statusCode).toBe(503);
      const body = JSON.parse(req3.body);
      expect(body.error.type).toBe("service_unavailable");
      expect(body.error.code).toBe("admission_full");

      held1.release();
      held2.release();
      const [res1, res2] = await Promise.all([req1, req2]);
      expect(res1.statusCode).toBe(200);
      expect(res2.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("an auth refusal returns its pre-auth slot", async () => {
    const tenantId = randomUUID();
    const fakeDb = fakeAuthDb(tenantId);

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePre: createAdmissionGate(1),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          return { ok: true };
        });
      },
    });

    try {
      const refused = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: "Bearer not-a-key" },
      });
      expect(refused.statusCode).toBe(401);

      // With a single PRE slot, this succeeds only if the 401 returned it.
      const admitted = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      expect(admitted.statusCode).toBe(200);
    } finally {
      await app.close();
    }
  });

  it("the handoff returns the pre-auth slot while the handler still runs", async () => {
    const tenantId = randomUUID();
    const fakeDb = fakeAuthDb(tenantId);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let signalParked!: () => void;
    const parkedSignal = new Promise<void>((resolve) => {
      signalParked = resolve;
    });

    let entries = 0;
    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGatePre: createAdmissionGate(1),
      admissionGatePost: createAdmissionGate(2),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          entries++;
          if (entries === 1) {
            signalParked();
            await parked;
          }
          return { ok: true };
        });
      },
    });

    try {
      const req1 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      await parkedSignal;

      // req1 sits in the handler on a POST_AUTH slot. With a single PRE
      // slot, req2 passes the PRE gate only if the handoff returned req1's.
      const req2 = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      expect(req2.statusCode).toBe(200);

      open();
      const res1 = await req1;
      expect(res1.statusCode).toBe(200);
    } finally {
      open();
      await app.close();
    }
  });

  it("a post-auth-full refusal returns the pre-auth slot", async () => {
    const deferred = deferredAuthDb([
      randomUUID(),
      randomUUID(),
      randomUUID(),
    ]);

    let open!: () => void;
    const parked = new Promise<void>((resolve) => {
      open = resolve;
    });

    let signalParked!: () => void;
    const parkedSignal = new Promise<void>((resolve) => {
      signalParked = resolve;
    });

    let secondEntered!: () => void;
    const secondEnteredSignal = new Promise<void>((resolve) => {
      secondEntered = resolve;
    });

    let entries = 0;
    const app = await buildApp({
      logger: false,
      db: deferred.db,
      admissionGatePre: createAdmissionGate(1),
      admissionGatePost: createAdmissionGate(1),
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          entries++;
          if (entries === 1) {
            signalParked();
          }
          if (entries === 2) {
            secondEntered();
          }
          await parked;
          return { ok: true };
        });
      },
    });

    try {
      const req1 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      const held1 = await deferred.reached(1);
      held1.release();
      await parkedSignal; // req1 now holds the only POST_AUTH slot

      // req2 reaching the auth seam proves req1's handoff returned the PRE
      // slot; resolving early instead means it was refused at the PRE gate.
      const req2 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      let req2GuardArmed = true;
      const req2RefusedEarly = new Promise<never>((_, reject) => {
        void req2.then((res) => {
          if (req2GuardArmed) {
            reject(
              new Error(
                `request 2 was refused before the auth lookup (${res.statusCode}) — ` +
                  "request 1's handoff never returned its PRE_AUTH slot",
              ),
            );
          }
        });
      });
      const held2 = await Promise.race([
        deferred.reached(2),
        req2RefusedEarly,
      ]);
      req2GuardArmed = false;
      held2.release();

      // POST budget full: req2 must be refused, never enter the handler.
      const res2 = await Promise.race([
        req2,
        secondEnteredSignal.then((): never => {
          throw new Error(
            "a second request entered the handler while the post-auth budget was full",
          );
        }),
      ]);
      expect(res2.statusCode).toBe(503);
      const body2 = JSON.parse(res2.body);
      expect(body2.error.type).toBe("service_unavailable");
      expect(body2.error.code).toBe("admission_full");

      // req3 reaching the auth seam proves req2's refusal returned the PRE
      // slot in turn.
      const req3 = app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });
      let req3GuardArmed = true;
      const req3RefusedEarly = new Promise<never>((_, reject) => {
        void req3.then((res) => {
          if (req3GuardArmed) {
            reject(
              new Error(
                `request 3 was refused before the auth lookup (${res.statusCode}) — ` +
                  "request 2's post-auth-full refusal never returned its PRE_AUTH slot",
              ),
            );
          }
        });
      });
      const held3 = await Promise.race([
        deferred.reached(3),
        req3RefusedEarly,
      ]);
      req3GuardArmed = false;
      held3.release();

      const res3 = await Promise.race([
        req3,
        secondEnteredSignal.then((): never => {
          throw new Error(
            "a second request entered the handler while the post-auth budget was full",
          );
        }),
      ]);
      expect(res3.statusCode).toBe(503);

      open();
      const res1 = await req1;
      expect(res1.statusCode).toBe(200);
    } finally {
      open();
      await app.close();
    }
  });

  it("production default pre-auth capacity is 10 — behavioral pin", async () => {
    const tenantIds = Array.from({ length: 10 }, () => randomUUID());
    const deferred = deferredAuthDb(tenantIds);

    const app = await buildApp({
      logger: false,
      db: deferred.db,
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          return { ok: true };
        });
      },
    });

    try {
      const requests = [];
      const held = [];
      for (let i = 0; i < 10; i++) {
        requests.push(
          app.inject({
            method: "GET",
            url: "/probe",
            headers: { authorization: bearer() },
          }),
        );
        held.push(await deferred.reached(i + 1));
      }

      // All 10 PRE slots held at the auth seam; the 11th must be refused
      // before lookup #11 (deferredAuthDb throws on it, failing named).
      const extraReq = await app.inject({
        method: "GET",
        url: "/probe",
        headers: { authorization: bearer() },
      });

      expect(extraReq.statusCode).toBe(503);
      const body = JSON.parse(extraReq.body);
      expect(body.error.type).toBe("service_unavailable");
      expect(body.error.code).toBe("admission_full");

      for (const lookup of held) {
        lookup.release();
      }
      const results = await Promise.all(requests);
      for (const res of results) {
        expect(res.statusCode).toBe(200);
      }
    } finally {
      await app.close();
    }
  });
});
