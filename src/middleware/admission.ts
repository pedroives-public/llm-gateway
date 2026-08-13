// Admission as a per-phase bulkhead: a request wins a PRE_AUTH slot before
// authentication, then trades it for a POST_AUTH slot once identified. The
// budgets are separate because the anonymous population is adversary-sized
// while tenants are operator-provisioned: an anonymous flood can exhaust
// only the pre-auth budget. Refusal never touches the circuit breaker. The
// held slot's release closure lives on the request and is wired to every
// terminal event; its idempotent flag makes multi-event wiring safe.

import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import type { AdmissionGate } from "../reliability/admission-gate.js";

export interface AdmissionPolicy {
  preGate: AdmissionGate;
  postGate: AdmissionGate;
  authenticate: (request: FastifyRequest, reply: FastifyReply) => Promise<void>;
}

// 503 payload mirrors the circuit-breaker fast-fail shape (the shared
// "service_unavailable" lane). Message stays generic: no capacity numbers
// and no hint of WHICH budget refused — policy is not client contract.
function refuse(reply: FastifyReply): FastifyReply {
  return reply.code(503).send({
    error: {
      message: "Service temporarily unavailable",
      type: "service_unavailable",
      code: "admission_full",
    },
  });
}

export function registerAdmission(
  scope: FastifyInstance,
  policy: AdmissionPolicy,
): void {
  // Registration order IS the request timeline: PRE gate, authenticate,
  // PRE->POST handoff — owning the sequence here keeps "auth runs between
  // the gates" structural. When authenticate() refuses, Fastify skips the
  // remaining onRequest hooks and the terminal events return the PRE slot.
  scope.addHook("onRequest", async (request, reply) => {
    const releasePreSlot = policy.preGate.tryAcquire();

    if (releasePreSlot === null) {
      return refuse(reply);
    }

    request.releaseSlot = releasePreSlot;
  });

  scope.addHook("onRequest", policy.authenticate);

  scope.addHook("onRequest", async (request, reply) => {
    const releasePostSlot = policy.postGate.tryAcquire();

    if (releasePostSlot === null) {
      return refuse(reply);
    }

    request.releaseSlot?.();
    request.releaseSlot = releasePostSlot;
  });

  scope.addHook("onResponse", async (request) => {
    request.releaseSlot?.();
  });

  scope.addHook("onRequestAbort", async (request) => {
    request.releaseSlot?.();
  });
}
