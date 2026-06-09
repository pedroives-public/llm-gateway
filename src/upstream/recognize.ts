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
    };

function isSuccessStatus(status: number): boolean {
  return status >= 200 && status < 300;
}

export function recognize(raw: RawFacts): Outcome {
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
