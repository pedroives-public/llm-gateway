import { describe, it, expect } from "vitest";
import { createAdmissionGate } from "../../src/reliability/admission-gate.js";

describe("createAdmissionGate — slot accounting", () => {
  it("admits requests up to capacity", () => {
    const capacity = 3;
    const gate = createAdmissionGate(capacity);

    const slots: ReturnType<typeof gate.tryAcquire>[] = [];
    for (let i = 0; i < capacity; i++) {
      const slot = gate.tryAcquire();
      expect(slot).not.toBeNull();
      slots.push(slot);
    }

    for (const slot of slots) {
      expect(slot).toBeTypeOf("function");
    }
  });

  it("refuses the next request once capacity is full", () => {
    const capacity = 3;
    const gate = createAdmissionGate(capacity);

    const slots: ReturnType<typeof gate.tryAcquire>[] = [];
    for (let i = 0; i < capacity; i++) {
      const slot = gate.tryAcquire();
      expect(slot).not.toBeNull();
      slots.push(slot);
    }

    const extraSlot = gate.tryAcquire();
    expect(extraSlot).toBeNull();
  });

  it("a released slot is immediately available to the next request", () => {
    const capacity = 3;
    const gate = createAdmissionGate(capacity);

    const slots: ReturnType<typeof gate.tryAcquire>[] = [];
    for (let i = 0; i < capacity; i++) {
      const slot = gate.tryAcquire();
      expect(slot).not.toBeNull();
      slots.push(slot);
    }

    // Release one slot
    const releasedSlot = slots[0];
    releasedSlot?.();

    // Now the next request should succeed
    const newSlot = gate.tryAcquire();
    expect(newSlot).toBeTypeOf("function");
  });

  it("releasing the same slot twice does not mint a phantom slot", () => {
    const capacity = 3;
    const gate = createAdmissionGate(capacity);

    const slots: ReturnType<typeof gate.tryAcquire>[] = [];
    for (let i = 0; i < capacity; i++) {
      const slot = gate.tryAcquire();
      expect(slot).not.toBeNull();
      slots.push(slot);
    }

    // Release one slot twice
    const releasedSlot = slots[0];
    releasedSlot?.();
    releasedSlot?.(); // Second release should be a no-op

    // Now the next request should succeed, but the one after that should fail
    const newSlot1 = gate.tryAcquire();
    expect(newSlot1).toBeTypeOf("function");

    const newSlot2 = gate.tryAcquire();
    expect(newSlot2).toBeNull();
  });

  it("rejects a non-positive or non-integer capacity at construction", () => {
    // NaN is the dangerous corner: `inFlight >= NaN` is always false, so an
    // unvalidated NaN capacity would fail OPEN (admit everything, unbounded).
    // Zero/negative merely fail closed; the guard kills both directions at boot.
    expect(() => createAdmissionGate(0)).toThrow(RangeError);
    expect(() => createAdmissionGate(-1)).toThrow(RangeError);
    expect(() => createAdmissionGate(Number.NaN)).toThrow(RangeError);
    expect(() => createAdmissionGate(2.5)).toThrow(RangeError);
  });
});
