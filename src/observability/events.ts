import type { PlanTier } from "../db/schema.js";

export type ErrorClass =
  | "client-fault"
  | "gateway-fault"
  | "upstream-fault"
  | "upstream-retry-exhausted";

export type CbState = "CLOSED" | "OPEN" | "HALF_OPEN";

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
  duration_ms: number;
  upstream_duration_ms: number;
  gateway_overhead_ms: number;
  attempts: number;
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
