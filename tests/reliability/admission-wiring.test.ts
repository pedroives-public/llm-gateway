import { describe, it, expect } from "vitest";
import { randomBytes, randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import type { AdmissionGate } from "../../src/reliability/admission-gate.js";
import { buildApp } from "../../src/app.js";
import { fakeAuthDb } from "../helpers/fake-auth-db.js";
import { createAdmissionGate } from "../../src/reliability/admission-gate.js";

// Integration cells for the admission gate wired into the protected scope.
// House pattern: buildApp({ registerProtected, admissionGate }) with fakeAuthDb,
// a test route whose handler parks on a deferred promise (requests stay
// in-flight until the test resolves it), and app.inject() calls fired
// concurrently WITHOUT await — await only the assertions.
// Cells 1-4 inject a tiny gate (capacity 2); the last cell builds the DEFAULT
// app and proves the production capacity behaviorally (41 parked, 42nd -> 503).
// Convert each it.todo into it() as you implement.

describe("admission wiring — protected scope", () => {
  it("admits authenticated requests while slots are free", async () => {
    const tenantId = randomUUID();
    const apiKey = `lkey_${randomBytes(32).toString("base64url")}`;

    const fakeDb = fakeAuthDb(tenantId);

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      admissionGate: createAdmissionGate(2),
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
      admissionGate: createAdmissionGate(2),
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
      admissionGate: createAdmissionGate(2),
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
      admissionGate: createAdmissionGate(2),
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

    let parkedCount = 0;
    let allParked!: () => void;
    const allParkedSignal = new Promise<void>((resolve) => {
      allParked = resolve;
    });

    const app = await buildApp({
      logger: false,
      db: fakeDb,
      registerProtected: async (scope) => {
        scope.get("/probe", async () => {
          parkedCount++;
          if (parkedCount === 41) {
            allParked();
          }
          await parked;
          return { ok: true };
        });
      },
    });

    try {
      const requests = [];
      for (let i = 0; i < 41; i++) {
        requests.push(
          app.inject({
            method: "GET",
            url: "/probe",
            headers: { authorization: `Bearer ${apiKey}` },
          }),
        );
      }

      // Fail fast if any parked request resolves before release — with the
      // correct capacity all 41 park; an early 503 means capacity < 41. The
      // guard is disarmed before open(), when resolving becomes the expected
      // outcome (otherwise it would raise an orphan rejection on the happy path).
      let guardArmed = true;
      const anyResolvedEarly = Promise.race(requests).then((res): void => {
        if (guardArmed) {
          throw new Error(
            `a parked request resolved before release: ${res.statusCode}`,
          );
        }
      });

      // Wait until all 41 requests are parked (in-flight)
      await Promise.race([allParkedSignal, anyResolvedEarly]);
      guardArmed = false;

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
      admissionGate: spyGate,
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
