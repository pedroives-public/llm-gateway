import type { Outcome } from "./outcome.js";
import { extractCauseLog } from "./cause-log.js";
import { recognizeRejection, type RejectionFacts } from "./recognize.js";

export type Logger = {
  error(payload: Record<string, unknown>, msg: string): void;
};

const UNKNOWN = "UNKNOWN";

function extractErrName(err: unknown): string {
  if (
    typeof err === "object" &&
    err !== null &&
    "name" in err &&
    typeof err.name === "string"
  ) {
    return err.name;
  }
  return UNKNOWN;
}

function extractCauseCode(err: unknown): string | undefined {
  if (
    typeof err === "object" &&
    err !== null &&
    "cause" in err &&
    typeof err.cause === "object" &&
    err.cause !== null &&
    "code" in err.cause &&
    typeof err.cause.code === "string"
  ) {
    return err.cause.code;
  }
  return undefined;
}

function extractCauseName(err: unknown): string | undefined {
  if (
    typeof err === "object" &&
    err !== null &&
    "cause" in err &&
    typeof err.cause === "object" &&
    err.cause !== null &&
    "name" in err.cause &&
    typeof err.cause.name === "string"
  ) {
    return err.cause.name;
  }
  return undefined;
}

function extractAbortKind(err: unknown): string {
  if (
    typeof err === "object" &&
    err !== null &&
    "kind" in err &&
    typeof err.kind === "string"
  ) {
    return err.kind;
  }
  return UNKNOWN;
}

function logAndRethrow(
  err: unknown,
  abortIdentity: boolean,
  logger: Logger,
): never {
  logger.error(
    {
      ...extractCauseLog(err),
      err_name: extractErrName(err),
      abort_identity: abortIdentity,
    },
    "Unrecognized upstream rejection",
  );
  throw err;
}

export function resolveRejection(
  err: unknown,
  signal: AbortSignal,
  logger: Logger,
): Extract<Outcome, { kind: "network_failed" | "aborted" | "redirect_blocked" }> {
  const abortIdentity = err === signal.reason;
  let arm: RejectionFacts;
  let causeCode: string | undefined;
  let causeName: string | undefined;

  if (abortIdentity) {
    arm = {
      resolved: false,
      rejection: "abort",
      abort_kind: extractAbortKind(err),
    };
  } else {
    causeCode = extractCauseCode(err);
    if (causeCode === undefined) {
      logAndRethrow(err, abortIdentity, logger);
    }
    causeName = extractCauseName(err);

    arm = {
      resolved: false,
      rejection: "network",
      cause_code: causeCode,
    };
  }

  const outcome = recognizeRejection(arm);
  if (outcome.kind === "unrecognized") {
    logAndRethrow(err, abortIdentity, logger);
  }

  if (outcome.kind === "network_failed") {
    // Enrich with the SAME proven cause that passed the eligibility guard
    // above — a single extraction, reused. The stored code is, by
    // construction, the one the recognizer accepted, not a second read that
    // could drift from it.
    return {
      ...outcome,
      cause_code: causeCode,
      cause_name: causeName,
    };
  }

  return outcome;
}
