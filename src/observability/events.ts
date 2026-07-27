import type { PlanTier } from "../db/schema.js";

export type ErrorClass =
  | "client-fault"
  | "gateway-fault"
  | "upstream-fault"
  | "upstream-retry-exhausted"
  | "upstream-auth-failure"
  | "upstream-access-denied"
  | "upstream-quota-exhausted";

// Why a request did or did not retry, reconstructed at wiring time for incident
// triage. `skipped_budget` means STRICTLY a Retry-After-vs-budget skip; an
// eligible outcome that coincided with a gateway abort is `ineligible`, not a
// budget skip, so the label stays single-meaning for SLI filters.
export type RetryDisposition = "attempted" | "skipped_budget" | "ineligible";

export type CbState = "CLOSED" | "OPEN" | "HALF_OPEN";

export type ReqRejectedReason = "schema_validation" | "cost_cap_exceeded";

type UpstreamAuthAlertLine = {
  event: "operational_alert";
  alert: "upstream_auth_failure";
  req_id: string;
};

type UpstreamQuotaAlertLine = {
  event: "operational_alert";
  alert: "upstream_quota_exhausted";
  req_id: string;
};

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

export interface UpstreamAuthAlertPayload {
  req_id: string;
}

export interface UpstreamQuotaAlertPayload {
  req_id: string;
}

export interface CbStateChangePayload {
  from: CbState;
  to: CbState;
  failure_count: number;
  window_start_ms: number;
}

type MinLogger = { info: (obj: object) => void };
type AlertMinLogger = { error: (obj: UpstreamAuthAlertLine) => void };
type QuotaAlertMinLogger = { error: (obj: UpstreamQuotaAlertLine) => void };

let _firstRequestCompleted = false;
let _upstreamAuthAlertFired = false;
let _upstreamQuotaAlertFired = false;

export function wasColdStart(): boolean {
  return !_firstRequestCompleted;
}

export function _markFirstRequestCompleted(): void {
  _firstRequestCompleted = true;
}

/**
 * Request admitted past auth. Anchor event the other per-request events join
 * against via `req_id`; carries tenant/plan/stream/cold-start context.
 */
export function emitReqStart(log: MinLogger, payload: ReqStartPayload): void {
  log.info({ event: "req_start", ...payload });
}

/**
 * Terminal event for a non-streaming request (or a streaming one that failed
 * before its first SSE token). Input to the success-rate and non-streaming
 * latency SLIs; also ends the process's cold-start window.
 */
export function emitReqComplete(
  log: MinLogger,
  payload: ReqCompletePayload,
): void {
  log.info({ event: "req_complete", ...payload });
  _markFirstRequestCompleted();
}

/**
 * Gateway-side rejection before any upstream attempt (schema validation or
 * cost cap). Its own event so rejections never enter the upstream
 * success-rate population.
 */
export function emitReqRejected(
  log: MinLogger,
  payload: ReqRejectPayload,
): void {
  log.info({ event: "req_rejected", ...payload });
}

/**
 * First SSE token flushed to the client on a streaming request. Measurement
 * point for the TTFT percentile SLI (streaming path only).
 */
export function emitStreamFirstToken(
  log: MinLogger,
  payload: StreamFirstTokenPayload,
): void {
  log.info({ event: "stream_first_token", ...payload });
}

/**
 * Terminal event for a streaming request that flushed at least one token
 * (`completed` marks clean end vs mid-stream break). Input to the streaming
 * SLIs; also ends the process's cold-start window.
 */
export function emitStreamDone(
  log: MinLogger,
  payload: StreamDonePayload,
): void {
  log.info({ event: "stream_done", ...payload });
  _markFirstRequestCompleted();
}

/**
 * Circuit-breaker FSM transition (CLOSED/OPEN/HALF_OPEN). Operational
 * timeline for incidents; not an SLI input.
 */
export function emitCbStateChange(
  log: MinLogger,
  payload: CbStateChangePayload,
): void {
  log.info({ event: "cb_state_change", ...payload });
}

/**
 * Once-per-process operator alert: the deployment's own upstream credential
 * was rejected (401). Alerts summon, events describe — later occurrences
 * stay visible via req_complete's error_class.
 */
export function emitUpstreamAuthAlert(
  log: AlertMinLogger,
  payload: UpstreamAuthAlertPayload,
): void {
  if (_upstreamAuthAlertFired) {
    return;
  }

  _upstreamAuthAlertFired = true;

  log.error({
    event: "operational_alert",
    alert: "upstream_auth_failure",
    ...payload,
  });
}

/**
 * Once-per-process operator alert: the deployment's upstream account ran out
 * of quota (429 insufficient_quota). Same summon-once contract as the auth
 * alert.
 *
 * Deliberate near-duplicate of emitUpstreamAuthAlert: two alerts is below the
 * abstraction threshold, and per-alert Line types keep the log allowlist
 * explicit. Generalize into a single emitter when a third alert lands.
 */
export function emitUpstreamQuotaAlert(
  log: QuotaAlertMinLogger,
  payload: UpstreamQuotaAlertPayload,
): void {
  if (_upstreamQuotaAlertFired) {
    return;
  }

  _upstreamQuotaAlertFired = true;

  log.error({
    event: "operational_alert",
    alert: "upstream_quota_exhausted",
    ...payload,
  });
}
