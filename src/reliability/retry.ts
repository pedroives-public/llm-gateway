import type { ErrorOutcome, Outcome } from "../upstream/outcome.js";
import { isRetryEligible, retryAfterMs } from "../upstream/retry-eligibility.js";

const RETRY_BACKOFF_MS = 100;

export function backoffCeiling(): number {
  return RETRY_BACKOFF_MS;
}

// Policy inputs for one logical request's retry envelope.
//
// `deadlineAt` is the absolute wall-clock deadline (epoch ms). The remaining
// budget is derived live at each decision point, never snapshotted — a snapshot
// would age during the first attempt. The AbortSignal carries no deadline.
//
// `firstByteFlushed` is a getter so it is read at the decision point, not at
// entry: it can flip true once the first streamed byte reaches the client.
export type RetryOptions = {
  signal: AbortSignal;
  deadlineAt: number;
  firstByteFlushed: () => boolean;
};

// Performs at most one retry per logical request (two attempts maximum).
// Consumes and returns an Outcome; it never throws to express a retry, budget,
// or abort decision. The only rejection that may escape is a re-thrown
// out-of-contract abort reason.
export async function retry(
  op: () => Promise<Outcome>,
  opts: RetryOptions,
): Promise<Outcome> {
  const first = await op();

  if (!shouldRetry(first, opts)) {
    return first;
  }

  const waitMs = retryAfterOrBackoffMs(first);
  const remainingMs = opts.deadlineAt - Date.now();

  if (waitMs >= remainingMs) {
    return first;
  }

  const abortDuringBackoff = await sleepBeforeRetry(opts, waitMs);
  if (abortDuringBackoff) {
    return abortDuringBackoff;
  }

  // Re-check before the second attempt: the deadline or first-byte flag may have
  // flipped during the backoff. `signal.aborted` is defensive — an abort mid-sleep
  // is normally caught by sleepBeforeRetry's listener, but not after the timer
  // removes it, so this keeps the guard correct regardless of that ordering.
  if (
    Date.now() >= opts.deadlineAt ||
    opts.firstByteFlushed() ||
    opts.signal.aborted
  ) {
    return first;
  }

  return op();
}

function shouldRetry(
  outcome: Outcome,
  opts: RetryOptions,
): outcome is ErrorOutcome {
  return (
    outcome.kind !== "ok" &&
    !opts.signal.aborted &&
    isRetryEligible(outcome) &&
    !opts.firstByteFlushed()
  );
}

function sleepBeforeRetry(
  opts: RetryOptions,
  waitMs: number,
): Promise<Outcome | undefined> {
  if (waitMs <= 0) {
    return Promise.resolve(undefined);
  }

  return new Promise((resolve, reject) => {
    const finish = (outcome?: Outcome) => {
      clearTimeout(timer);
      opts.signal.removeEventListener("abort", onAbort);
      resolve(outcome);
    };

    const onAbort = () => {
      try {
        finish(outcomeFromAbortReason(opts.signal.reason));
      } catch (error) {
        clearTimeout(timer);
        opts.signal.removeEventListener("abort", onAbort);
        reject(error);
      }
    };

    const timer = setTimeout(() => finish(), waitMs);
    opts.signal.addEventListener("abort", onAbort, { once: true });
  });
}

function outcomeFromAbortReason(reason: unknown): Outcome {
  if (isKnownAbortReason(reason)) {
    return { kind: "aborted", abort_kind: reason.kind };
  }

  throw reason;
}

function isKnownAbortReason(
  reason: unknown,
): reason is { kind: "response_size_cap" | "wall_clock_expired" } {
  return (
    typeof reason === "object" &&
    reason !== null &&
    "kind" in reason &&
    (reason.kind === "response_size_cap" ||
      reason.kind === "wall_clock_expired")
  );
}

// A usable Retry-After governs the wait; otherwise jittered backoff. The parse +
// validity rule lives in retryAfterMs (shared with the disposition label).
function retryAfterOrBackoffMs(outcome: Outcome): number {
  return retryAfterMs(outcome) ?? Math.random() * backoffCeiling();
}
