import type { CircuitBreaker } from "../../src/reliability/circuit-breaker.js";

// Always-admitting, never-recording breaker for tests where the breaker is
// not the subject. Stateless, so sharing one instance across tests is safe.
export const stubBreaker: CircuitBreaker = {
  tryAcquire: () => ({ kind: "NORMAL" }),
  recordResult: () => {},
  getState: () => "CLOSED",
};
