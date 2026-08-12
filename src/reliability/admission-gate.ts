// Admission gate: bounds simultaneous in-flight requests so process memory
// stays under the OOM ceiling. Slot ownership IS the release closure:
// tryAcquire() returns a release function bound to the one admitted request
// (or null when the gate is full), and an idempotent flag inside the closure
// makes the exactly-once return structural — a double call cannot mint a
// phantom slot, and no caller can release a slot it does not hold.

export type ReleaseSlot = () => void;

export interface AdmissionGate {
  /** Admit one request: the slot's release closure, or null when full. */
  tryAcquire(): ReleaseSlot | null;
}

export function createAdmissionGate(capacity: number): AdmissionGate {
  if (!Number.isInteger(capacity) || capacity <= 0) {
    throw new RangeError(
      `admission gate capacity must be a positive integer, got ${capacity}`,
    );
  }

  let inFlight = 0;

  function tryAcquire(): ReleaseSlot | null {
    // Check-then-increment is race-free only because this function is fully
    // synchronous; never introduce an await between the check and the increment.
    if (inFlight >= capacity) {
      return null;
    }

    inFlight++;
    let released = false;

    return function releaseSlot() {
      if (!released) {
        released = true;
        inFlight--;
      }
    };
  }

  return { tryAcquire };
}
