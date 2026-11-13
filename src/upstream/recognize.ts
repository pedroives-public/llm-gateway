import type { Outcome } from "./outcome.js";
import { assertNever } from "./assert-never.js";

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
    }
  | {
      resolved: false;
      rejection: "abort";
      abort_kind: string;
    };

export type UnrecognizedRejection = {
  kind: "unrecognized";
};

export type RejectionFacts = Extract<RawFacts, { resolved: false }>;
export type ResponseFacts = Extract<RawFacts, { resolved: true }>;
export type RejectionRecognition =
  | Extract<
      Outcome,
      { kind: "network_failed" | "aborted" | "redirect_blocked" }
    >
  | UnrecognizedRejection;
export type ResponseRecognition = Extract<
  Outcome,
  { kind: "ok" | "upstream_error" | "undecodable" | "upstream_3xx" }
>;
export type ResponseHeadRecognition =
  | Extract<Outcome, { kind: "upstream_3xx" }>
  | { kind: "not_3xx" };

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

function is3xxStatus(status: number): boolean {
  return status >= 300 && status < 400;
}

export function recognizeResponseHead(status: number): ResponseHeadRecognition {
  if (!is3xxStatus(status)) {
    return { kind: "not_3xx" };
  }

  return { kind: "upstream_3xx", status };
}

function isRecognizedAbortKind(
  k: string,
): k is "response_size_cap" | "wall_clock_expired" | "total_timeout" {
  return (
    k === "response_size_cap" ||
    k === "wall_clock_expired" ||
    k === "total_timeout"
  );
}

export function recognizeRejection(raw: RejectionFacts): RejectionRecognition {
  switch (raw.rejection) {
    case "abort":
      if (isRecognizedAbortKind(raw.abort_kind)) {
        return { kind: "aborted", abort_kind: raw.abort_kind };
      }
      return { kind: "unrecognized" };
    case "network":
      if (PRE_SEND_PROVEN_CODES.includes(raw.cause_code)) {
        return { kind: "network_failed", pre_send_proven: true };
      }
      if (POSSIBLY_POST_SEND_CODES.includes(raw.cause_code)) {
        return { kind: "network_failed", pre_send_proven: false };
      }
      return { kind: "unrecognized" };
    default:
      return assertNever(raw);
  }
}

export function recognizeResponse(raw: ResponseFacts): ResponseRecognition {
  const recognizedHead = recognizeResponseHead(raw.status);

  if (recognizedHead.kind === "upstream_3xx") {
    return recognizedHead;
  }

  if (raw.parsed) {
    return { kind: "ok", status: raw.status, body_parsed: raw.body_parsed };
  }

  if (isSuccessStatus(raw.status)) {
    return { kind: "undecodable", undecodable_kind: "unparsable_body" };
  }

  return {
    kind: "upstream_error",
    status: raw.status,
    body_raw: raw.body_raw,
    retry_after: raw.retry_after,
  };
}

export function recognize(raw: RawFacts): Outcome | UnrecognizedRejection {
  if (!raw.resolved) {
    return recognizeRejection(raw);
  }

  return recognizeResponse(raw);
}
