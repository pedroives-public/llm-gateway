// Admission control for the authenticated scope: a request must win a slot
// before its handler runs; the gate refuses with 503 when the process is at
// capacity. Refusal is not a failure signal — it never touches the circuit
// breaker. The release closure is stored on the request and wired to every
// terminal event; its idempotent flag makes multi-event wiring safe (Fastify
// can fire more than one end event for the same request).

import type { FastifyInstance } from "fastify";
import type { AdmissionGate } from "../reliability/admission-gate.js";

// 503 payload mirrors the circuit-breaker fast-fail shape (the shared
// "service_unavailable" lane). Message stays generic: no capacity numbers,
// no counters — policy values are not part of the client contract.

export function registerAdmission(
  scope: FastifyInstance,
  gate: AdmissionGate,
): void {
  scope.addHook("onRequest", async (request, reply) => {
    const releaseSlot = gate.tryAcquire();

    if (releaseSlot === null) {
      return reply.code(503).send({
        error: {
          message: "Service temporarily unavailable",
          type: "service_unavailable",
          code: "admission_full",
        },
      });
    }

    request.releaseSlot = releaseSlot;
  });

  scope.addHook("onResponse", async (request) => {
    request.releaseSlot?.();
  });

  scope.addHook("onRequestAbort", async (request) => {
    request.releaseSlot?.();
  });
}
