export type CauseLog = { cause_code: string; cause_name: string };

export function extractCauseLog(err: unknown): CauseLog {
  if (
    typeof err === "object" &&
    err !== null &&
    "cause" in err &&
    typeof err.cause === "object" &&
    err.cause !== null &&
    "code" in err.cause &&
    typeof err.cause.code === "string" &&
    "name" in err.cause &&
    typeof err.cause.name === "string"
  ) {
    return { cause_code: err.cause.code, cause_name: err.cause.name };
  }
  return { cause_code: "UNKNOWN", cause_name: "UNKNOWN" };
}
