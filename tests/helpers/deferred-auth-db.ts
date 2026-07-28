import type { DrizzleClient } from "../../src/db/client.js";

// Auth-lookup seam that parks every call at the database boundary until the
// test releases it, one distinct tenant row per call; fake-auth-db.ts instead
// resolves immediately and binds a single tenant. Every call parks — not just
// the first — because an interleaving that relies on which lookup is slower is
// a race, and a race that passes cannot tell "the defence held" from "it never
// interleaved".

export interface HeldLookup {
  // Resume the request suspended at this lookup, with the row assigned to it.
  release: () => void;
}

export interface DeferredAuthDb {
  db: DrizzleClient;
  // Settles once the nth lookup (1-indexed) reaches the seam; awaiting it
  // before driving the next request makes the ordering enforced, not assumed.
  reached: (n: number) => Promise<HeldLookup>;
}

interface Signal {
  promise: Promise<void>;
  fire: () => void;
}

function signal(): Signal {
  let fire: () => void = () => {};
  const promise = new Promise<void>((resolve) => {
    fire = resolve;
  });
  return { promise, fire };
}

export function deferredAuthDb(tenantIds: readonly string[]): DeferredAuthDb {
  const arrived = tenantIds.map(() => signal());
  const released = tenantIds.map(() => signal());
  let calls = 0;

  const limit = async (): Promise<unknown[]> => {
    const index = calls;
    calls += 1;

    const tenantId = tenantIds[index];
    const arrival = arrived[index];
    const release = released[index];

    if (
      tenantId === undefined ||
      arrival === undefined ||
      release === undefined
    ) {
      throw new Error(`deferredAuthDb: unexpected auth lookup #${index + 1}`);
    }

    arrival.fire();
    await release.promise;

    return [
      {
        tenantId,
        planTier: "pro",
        apiKeyStatus: "active",
        tenantStatus: "active",
      },
    ];
  };

  // Mirrors the fluent chain authPreHandler builds; the where clause is
  // ignored, so which row a call receives follows call order, not the key
  // presented.
  const db = {
    select: () => ({
      from: () => ({
        innerJoin: () => ({
          where: () => ({ limit }),
        }),
      }),
    }),
  } as unknown as DrizzleClient;

  const reached = async (n: number): Promise<HeldLookup> => {
    const arrival = arrived[n - 1];
    const release = released[n - 1];

    if (arrival === undefined || release === undefined) {
      throw new Error(`deferredAuthDb: no lookup #${n} was declared`);
    }

    await arrival.promise;
    return { release: release.fire };
  };

  return { db, reached };
}
