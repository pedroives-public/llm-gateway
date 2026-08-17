import type { PlanTier } from "../db/schema.js";
import { assertNever } from "../upstream/assert-never.js";

// Operator-page alert names. The array is the single source: AlertName
// derives from it, and the unit suite iterates it — an alert added here is
// covered by the exactly-once and independence cells automatically.
export const ALERT_NAMES = [
  // the deployment's own upstream credential was rejected (401)
  "upstream_auth_failure",
  // the deployment's upstream account ran out of quota (insufficient_quota)
  "upstream_quota_exhausted",
  // the configured endpoint answered 3xx and the gateway refuses to follow it
  "upstream_redirect_blocked",
  // the auth DB catch saw an error whose shape the cause vocabulary cannot
  // identify (cause_code or cause_name fell back to UNKNOWN)
  "auth_db_cause_unknown",
] as const;

export type ErrorClass =
  | "client-fault"
  | "gateway-fault"
  | "upstream-fault"
  | "upstream-retry-exhausted"
  | "upstream-auth-failure"
  | "upstream-access-denied"
  | "upstream-quota-exhausted"
  | "upstream-redirect-blocked";

// Why a request did or did not retry, reconstructed at wiring time for incident
// triage. `skipped_budget` means STRICTLY a Retry-After-vs-budget skip; an
// eligible outcome that coincided with a gateway abort is `ineligible`, not a
// budget skip, so the label stays single-meaning for SLI filters.
export type RetryDisposition = "attempted" | "skipped_budget" | "ineligible";

export type CbState = "CLOSED" | "OPEN" | "HALF_OPEN";

export type ReqRejectedReason = "schema_validation" | "cost_cap_exceeded";

export type AlertName = (typeof ALERT_NAMES)[number];

type OperationalAlertLine = {
  event: "operational_alert";
  alert: AlertName;
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

export interface OperationalAlertPayload {
  req_id: string;
}

export interface CbStateChangePayload {
  from: CbState;
  to: CbState;
  failure_count: number;
  window_start_ms: number;
}

type MinLogger = { info: (obj: object) => void };
type OperationalAlertMinLogger = { error: (obj: OperationalAlertLine) => void };

let _firstRequestCompleted = false;
const _firedAlerts = new Set<AlertName>();

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
 * Once-per-process operator alert, keyed by alert name: the first occurrence
 * of each name summons the operator. Alerts summon, events describe — later
 * occurrences stay visible via req_complete's error_class. What each name
 * means lives on the ALERT_NAMES entries.
 */
export function emitOperationalAlert(
  log: OperationalAlertMinLogger,
  alert: AlertName,
  payload: OperationalAlertPayload,
): void {
  if (_firedAlerts.has(alert)) {
    return;
  }

  _firedAlerts.add(alert);

  log.error({
    event: "operational_alert",
    alert,
    ...payload,
  });
}

/**
 * Decides which error classes summon the operator. Exhaustive on purpose: a
 * new ErrorClass refuses to compile until it declares its alert here — an
 * AlertName or an explicit null.
 *
 * This mapping is one of two summoning doors: a named site may call
 * emitOperationalAlert directly when its signal is not an ErrorClass — the
 * first such site is the auth DB catch's vocabulary miss
 * (auth_db_cause_unknown).
 */
export function alertFor(errorClass: ErrorClass): AlertName | null {
  switch (errorClass) {
    case "upstream-auth-failure":
      return "upstream_auth_failure";
    case "upstream-quota-exhausted":
      return "upstream_quota_exhausted";
    case "upstream-redirect-blocked":
      return "upstream_redirect_blocked";

    case "client-fault":
    case "gateway-fault":
    case "upstream-fault":
    case "upstream-retry-exhausted":
    case "upstream-access-denied":
      return null;
    default:
      return assertNever(errorClass);
  }
}
