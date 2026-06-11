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
): Extract<Outcome, { kind: "network_failed" | "aborted" }> {
  const abortIdentity = err === signal.reason;
  let arm: RejectionFacts;

  if (abortIdentity) {
    arm = {
      resolved: false,
      rejection: "abort",
      abort_kind: extractAbortKind(err),
    };
  } else {
    const causeCode = extractCauseCode(err);
    if (causeCode === undefined) {
      logAndRethrow(err, abortIdentity, logger);
    }

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

  return outcome;
}
