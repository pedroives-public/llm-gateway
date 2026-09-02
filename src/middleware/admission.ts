// Admission as a per-phase bulkhead: a request wins a PRE_AUTH slot before
// authentication, then trades it for a POST_AUTH slot once identified. The
// budgets are separate because the anonymous population is adversary-sized
// while tenants are operator-provisioned: an anonymous flood can exhaust
// only the pre-auth budget. Refusal never touches the circuit breaker. The
// held slot's release closure lives on the request, anchored on the
// socket-level close plus a destroyed check at the post-auth acquisition;
// its idempotent flag makes the redundant framework hooks harmless.

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

    // Socket-level close fires on every termination; the hooks below do
    // not (onResponse needs a finished response, onRequestAbort an unread
    // body). Read request.releaseSlot at event time: after the handoff it
    // is the POST slot. Covers a close that happens AFTER this point.
    reply.raw.on("close", () => {
      request.releaseSlot?.();
    });
  });

  scope.addHook("onRequest", policy.authenticate);

  scope.addHook("onRequest", async (request, reply) => {
    const releasePostSlot = policy.postGate.tryAcquire();

    if (releasePostSlot === null) {
      return refuse(reply);
    }

    request.releaseSlot?.();
    request.releaseSlot = releasePostSlot;

    // A socket that died during auth has already emitted close: a listener
    // attached now never fires, but the destroyed flag persists. Keep this
    // check synchronous with the acquisition: the release is guaranteed
    // here, not when some later await returns.
    if (reply.raw.destroyed) {
      request.releaseSlot?.();
    }
  });

  scope.addHook("onResponse", async (request) => {
    request.releaseSlot?.();
  });

  scope.addHook("onRequestAbort", async (request) => {
    request.releaseSlot?.();
  });
}
