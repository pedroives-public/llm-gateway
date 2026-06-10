import type { Outcome } from "./outcome.js";

export type RawFacts =
  | {
      resolved: true;
      parsed: true;
      status: number;
      body_parsed: unknown;
    }
  | {
      resolved: true;
      parsed: false;
      status: number;
      body_raw: string;
      retry_after?: string;
    }
  | {
      resolved: false;
      rejection: "network";
      cause_code: string;
    };

export type UnrecognizedRejection = {
  kind: "unrecognized";
};

const PRE_SEND_PROVEN_CODES: readonly string[] = [
  "ENOTFOUND",
  "ECONNREFUSED",
  "UND_ERR_CONNECT_TIMEOUT",
];
const POSSIBLY_POST_SEND_CODES: readonly string[] = [
  "ECONNRESET",
  "ETIMEDOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_BODY_TIMEOUT",
  "EPIPE",
  "UND_ERR_SOCKET",
];

function isSuccessStatus(status: number): boolean {
  return status >= 200 && status < 300;
}

export function recognize(raw: RawFacts): Outcome | UnrecognizedRejection {
  if (!raw.resolved) {
    if (PRE_SEND_PROVEN_CODES.includes(raw.cause_code)) {
      return { kind: "network_failed", pre_send_proven: true };
    }
    if (POSSIBLY_POST_SEND_CODES.includes(raw.cause_code)) {
      return { kind: "network_failed", pre_send_proven: false };
    }

    return { kind: "unrecognized" };
  }
  if (raw.parsed) {
    return { kind: "ok", status: raw.status, body_parsed: raw.body_parsed };
  }
  if (isSuccessStatus(raw.status)) {
    return { kind: "undecodable" };
  }
  return {
    kind: "upstream_error",
    status: raw.status,
    body_raw: raw.body_raw,
    retry_after: raw.retry_after,
  };
}
