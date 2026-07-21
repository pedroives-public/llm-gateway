import type { PlanTier } from "../db/schema.js";

export type ErrorClass =
  | "client-fault"
  | "gateway-fault"
  | "upstream-fault"
  | "upstream-retry-exhausted";

// Why a request did or did not retry, reconstructed at wiring time for incident
// triage. `skipped_budget` means STRICTLY a Retry-After-vs-budget skip; an
// eligible outcome that coincided with a gateway abort is `ineligible`, not a
// budget skip, so the label stays single-meaning for SLI filters.
export type RetryDisposition = "attempted" | "skipped_budget" | "ineligible";

export type CbState = "CLOSED" | "OPEN" | "HALF_OPEN";

export type ReqRejectedReason = "schema_validation" | "cost_cap_exceeded";

export interface ReqStartPayload {
  req_id: string;
  route: string;
  tenant_id: string;
  plan_tier: PlanTier;
  stream: boolean;
  idempotency_key_present: boolean;
  was_cold_start: boolean;
}

export interface ReqCompletePayload {
  req_id: string;
  status: number;
  error_class: ErrorClass | null;
  stream: boolean;
  duration_ms: number;
  upstream_duration_ms: number;
  gateway_overhead_ms: number;
  attempts: number;
  retry_disposition: RetryDisposition;
}

export interface ReqRejectPayload {
  req_id: string;
  tenant_id: string;
  route: string | undefined;
  reason: ReqRejectedReason;
  status: number;
}

export interface StreamFirstTokenPayload {
  req_id: string;
  ttft_ms: number;
  was_cold_start: boolean;
}

export interface StreamDonePayload {
  req_id: string;
  completed: boolean;
  total_duration_ms: number;
  upstream_duration_ms: number;
  gateway_overhead_ms: number;
  total_tokens?: number;
  attempts: number;
  error_class: ErrorClass | null;
}

export interface CbStateChangePayload {
  from: CbState;
  to: CbState;
  failure_count: number;
  window_start_ms: number;
}

type MinLogger = { info: (obj: object) => void };

let _firstRequestCompleted = false;

export function wasColdStart(): boolean {
  return !_firstRequestCompleted;
}

export function _markFirstRequestCompleted(): void {
  _firstRequestCompleted = true;
}

export function emitReqStart(log: MinLogger, payload: ReqStartPayload): void {
  log.info({ event: "req_start", ...payload });
}

export function emitReqComplete(
  log: MinLogger,
  payload: ReqCompletePayload,
): void {
  log.info({ event: "req_complete", ...payload });
  _markFirstRequestCompleted();
}

export function emitReqRejected(
  log: MinLogger,
  payload: ReqRejectPayload,
): void {
  log.info({ event: "req_rejected", ...payload });
}

export function emitStreamFirstToken(
  log: MinLogger,
  payload: StreamFirstTokenPayload,
): void {
  log.info({ event: "stream_first_token", ...payload });
}

export function emitStreamDone(
  log: MinLogger,
  payload: StreamDonePayload,
): void {
  log.info({ event: "stream_done", ...payload });
  _markFirstRequestCompleted();
}

export function emitCbStateChange(
  log: MinLogger,
  payload: CbStateChangePayload,
): void {
  log.info({ event: "cb_state_change", ...payload });
}
