import { describe, it, expect } from "vitest";
import {
  DB_POOL_MAX,
  DEFAULT_DB_STATEMENT_TIMEOUT_MS,
} from "../../src/db/client.js";
import { ADMISSION_CAPACITY_PRE_AUTH } from "../../src/config.js";

// Re-computes the K_pre derivation from its inputs and asserts the shipped
// constant equals it: whoever changes an input (pool size, statement
// timeout, the residence tie) is forced to re-derive the capacity instead
// of silently running with a stale ceiling. Bounds: socket-buffer headroom
// floor(19 MiB / 1.38 MiB tcp_rmem max) = 13, and the auth-phase drain
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
    const memoryBound = Math.floor((19 * MiB) / (1.38 * MiB));
    // occupancy = throughput x residence: (pool / statement_timeout) x T_pre_max
    const drainChainBound = Math.floor(
      DB_POOL_MAX * (T_PRE_MAX_MS / DEFAULT_DB_STATEMENT_TIMEOUT_MS),
    );
    const K_pre_derivedCapacity = Math.min(memoryBound, drainChainBound);
    expect(ADMISSION_CAPACITY_PRE_AUTH).toEqual(K_pre_derivedCapacity);
  });
});
