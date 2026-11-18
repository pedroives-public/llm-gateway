import { describe, it, expect } from "vitest";
import {
  DB_POOL_MAX,
  DEFAULT_DB_STATEMENT_TIMEOUT_MS,
} from "../../src/db/client.js";
import {
  ADMISSION_CAPACITY_POST_AUTH,
  ADMISSION_CAPACITY_PRE_AUTH,
} from "../../src/config.js";
import { BODY_LIMIT_BYTES } from "../../src/routes/proxy.js";

// Re-computes the K_pre derivation from its inputs and asserts the shipped
// constant equals it: whoever changes an input (pool size, statement
// timeout, the residence tie) is forced to re-derive the capacity instead
// of silently running with a stale ceiling. Bounds: socket-buffer headroom
// floor(24 MiB / 1.38 MiB tcp_rmem max) = 17, and the auth-phase drain
// chain (throughput x residence); the smaller wins.

// Max pre-auth residence. Not a production constant: it is inherited from
// statement_timeout (the auth lookup is the phase's only long operation,
// so the query timeout IS the residence ceiling) and kept there by the
// zero-backlog decision. If a future change unties residence from the
// query timeout (e.g. accepting backlog), the ratio below starts moving
// the derived capacity.
const T_PRE_MAX_MS = 2000;
const MiB = 1024 * 1024;

describe("pre-auth capacity derivation pin", () => {
  it("K_pre equals the derivation recomputed from its inputs", () => {
    const memoryBound = Math.floor((24 * MiB) / (1.38 * MiB));
    // occupancy = throughput x residence: (pool / statement_timeout) x T_pre_max
    const drainChainBound = Math.floor(
      DB_POOL_MAX * (T_PRE_MAX_MS / DEFAULT_DB_STATEMENT_TIMEOUT_MS),
    );
    const K_pre_derivedCapacity = Math.min(memoryBound, drainChainBound);
    expect(ADMISSION_CAPACITY_PRE_AUTH).toEqual(K_pre_derivedCapacity);
  });
});

// Inputs of the post-auth derivation, all measured on 2026-11-18.
// MemAvailable on the deploy target, the lowest of three readings.
const MEM_AVAILABLE_MIB = 120.2;
const SAFETY_FACTOR = 0.85;
// Peak memory of one in-flight request whose body has MEASURED_BODY_BYTES.
const PER_REQUEST_MIB = 8.0;
// The body size that cost was measured with. Another limit has another cost,
// and only a new measurement gives it.
const MEASURED_BODY_BYTES = 1_572_864;

describe("post-auth capacity derivation pin", () => {
  it("the body limit is the size the per-request cost was measured with", () => {
    expect(
      BODY_LIMIT_BYTES,
      "the body limit moved without a new measurement of the per-request memory cost: measure the cost for the new size, then re-derive the post-auth capacity",
    ).toBe(MEASURED_BODY_BYTES);
  });

  it("K_post equals the derivation recomputed from its inputs", () => {
    const K_post_derivedCapacity = Math.floor(
      (MEM_AVAILABLE_MIB * SAFETY_FACTOR) / PER_REQUEST_MIB,
    );
    expect(
      ADMISSION_CAPACITY_POST_AUTH,
      "the post-auth capacity is not floor(MemAvailable x safety factor / per-request cost): re-derive it from the inputs of this pin",
    ).toBe(K_post_derivedCapacity);
  });
});
