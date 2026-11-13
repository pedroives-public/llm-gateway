import { getEventListeners } from "node:events";
import v8 from "node:v8";
import vm from "node:vm";

// A real garbage collection, without the --expose-gc flag on the runner.
v8.setFlagsFromString("--expose-gc");
export const collectGarbage = vm.runInNewContext("gc") as () => void;

// Captured at import, before a cell fakes setTimeout, so waits run on real time.
const realSetTimeout = globalThis.setTimeout;
export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => realSetTimeout(resolve, ms));

export const POLL_STEP_MS = 20;
// Real-time bound for the canary to be collected; a full collection is forced
// on every poll step, so a canary still alive at this bound means the
// instrument does not collect.
export const COLLECTION_DEADLINE_MS = 2_000;
// How long a cell keeps collecting after the canary is gone before it reads
// an abort-listener count. Under redirect "error" the runtime removed its
// listener at most 38 ms after the canary's collection (10 runs, Node 22.22.3,
// 20 ms poll step); the window is over ten times that, so an arm that keeps
// its listener is read only after a removal would have shown.
export const LISTENER_OBSERVATION_MS = 500;

// Rejects with `sentence` when `promise` has not settled within `ms` of real
// time; used where the defect is an absence (something that never ends).
export function settlesWithin<T>(
  promise: Promise<T>,
  ms: number,
  sentence: string,
): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => {
      realSetTimeout(() => reject(new Error(sentence)), ms);
    }),
  ]);
}

export async function waitFor(
  check: () => boolean,
  ms: number,
  sentence: string,
): Promise<void> {
  const startedAt = performance.now();
  while (!check()) {
    if (performance.now() - startedAt > ms) throw new Error(sentence);
    await sleep(POLL_STEP_MS);
  }
}

// An object nothing else references: once it is gone, a collection has run.
export function newCanary(): WeakRef<object> {
  return new WeakRef({});
}

// Collects until the canary is gone, then keeps collecting through the
// observation window, so a listener removal that follows the collection has
// shown before the caller reads a count.
export async function collectThroughWindow(
  canary: WeakRef<object>,
): Promise<void> {
  await waitFor(
    () => {
      collectGarbage();
      return canary.deref() === undefined;
    },
    COLLECTION_DEADLINE_MS,
    "the canary was not collected: there is no evidence that a garbage collection ran",
  );
  const canaryGoneAt = performance.now();
  while (performance.now() - canaryGoneAt < LISTENER_OBSERVATION_MS) {
    collectGarbage();
    await sleep(POLL_STEP_MS);
  }
}

// Abort listeners on `signal`; -1 when the scene has not handed one out yet.
export function abortListeners(signal: AbortSignal | undefined): number {
  return signal === undefined ? -1 : getEventListeners(signal, "abort").length;
}
