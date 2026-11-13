import type { StreamingUpstream } from "../upstream/openai.js";
import { cancelStream, isAcceptedStream } from "../upstream/stream.js";
import { createSseFrameReader } from "../upstream/sse-frame-reader.js";
import type { ProbeOutcome, ProbeVote } from "./circuit-breaker.js";
import { extractCauseLog } from "../upstream/cause-log.js";
import { extractErrName } from "../upstream/rejection.js";
import { PROBE_CLOCK_MS } from "../config.js";
import { armWallClockTimeout } from "./timeouts.js";
import {
  emitOperationalAlert,
  emitBreakerProbeClockExpired,
  type AlertName,
  type ProbeUpstreamReply,
} from "../observability/events.js";
import { isInsufficientQuota } from "../upstream/quota.js";
import type { ErrorOutcome } from "../upstream/outcome.js";
import { assertNever } from "../upstream/assert-never.js";

const PROBE_REQUEST_ID = "breaker_probe";

type ProbeDecision = {
  outcome: ProbeOutcome;
  alert?: AlertName;
};

function decideProbeOutcome(response: ErrorOutcome): ProbeDecision {
  switch (response.kind) {
    case "upstream_3xx":
      return {
        outcome: "FAILURE",
        alert: "upstream_redirect_blocked",
      };

    case "upstream_error":
      if (response.status === 401) {
        return { outcome: "FAILURE", alert: "upstream_auth_failure" };
      }

      if (isInsufficientQuota(response)) {
        return { outcome: "FAILURE", alert: "upstream_quota_exhausted" };
      }

      if (
        response.status === 400 ||
        response.status === 404 ||
        response.status === 413 ||
        response.status === 422
      ) {
        return { outcome: "SUCCESS", alert: "upstream_probe_rejected" };
      }

      if (response.status === 403) {
        return { outcome: "FAILURE", alert: "upstream_probe_access_denied" };
      }

      return { outcome: "FAILURE" };

    case "undecodable":
    case "network_failed":
    case "aborted":
      return { outcome: "FAILURE" };

    default:
      return assertNever(response);
  }
}

type ProbeRunnerDependencies = {
  streaming: StreamingUpstream;
  model: string;
  // One logger, two callers: the adapter logs with error(payload, msg), the
  // probe's alerts with error(line). The app's Pino logger serves both.
  log: Parameters<StreamingUpstream>[2] &
    Parameters<typeof emitOperationalAlert>[0];
};

type ProbeRequestBody = {
  model: string;
  messages: { role: "user"; content: string }[];
  stream: true;
  max_tokens: number;
};

export function createProbeRunner({
  streaming,
  model,
  log,
}: ProbeRunnerDependencies): (vote: ProbeVote, signal: AbortSignal) => Promise<void> {
  const requestBody: ProbeRequestBody = {
    model,
    messages: [{ role: "user", content: "ping" }],
    stream: true,
    max_tokens: 1,
  };

  return async (vote, signal) => {
    const timeout = armWallClockTimeout(PROBE_CLOCK_MS);
    const probeSignal = AbortSignal.any([timeout.signal, signal]);

    let upstreamReply: ProbeUpstreamReply = "not_started";
    let clockExpired = false;
    let readerCancelledByAbort = false;

    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let outcome: ProbeOutcome = "FAILURE";
    let alert: AlertName | undefined;
    let caughtError: { value: unknown } | undefined;

    // After a garbage collection, with redirect "error", the abort no longer
    // reaches a body read in progress (the runtime drops its abort listener).
    // Cancelling the reader still ends the read and closes the connection.
    const cancelReaderAfterAbort = (): void => {
      if (reader === undefined || readerCancelledByAbort) return;

      readerCancelledByAbort = true;
      void cancelStream(reader, log, "breaker_probe").catch(() => {
        // Only the cancel-rejected log line can reject here; the vote does
        // not depend on it.
      });
    };

    const reportClockExpired = (): void => {
      clockExpired = true;

      try {
        emitBreakerProbeClockExpired(log, {
          req_id: PROBE_REQUEST_ID,
          upstream_reply: upstreamReply,
        });
      } catch {
        // A logging failure must not prevent the abort cleanup or vote.
      }
    };

    timeout.signal.addEventListener("abort", reportClockExpired, {
      once: true,
    });
    // Registered before the request starts, so it runs before the runtime's
    // own abort listener: the cancel finds the body still open instead of
    // already errored.
    probeSignal.addEventListener("abort", cancelReaderAfterAbort, {
      once: true,
    });

    try {
      const response = await streaming(requestBody, probeSignal, log);

      if (probeSignal.aborted) {
        if (isAcceptedStream(response)) {
          reader = response.reader;
        }
      } else if (isAcceptedStream(response)) {
        reader = response.reader;
        upstreamReply = "not_finished";

        const nextFrame = createSseFrameReader(reader);

        while (true) {
          const frame = await nextFrame();

          // While the cancel works this does not fire: a timer never runs
          // between a read resolving and this continuation, and a read pending
          // when the clock fires ends with `done` once the clock cancels the
          // body. If the cancel ever fails, a later chunk (a keepalive) still
          // ends the loop here, so a frame never outvotes a clock that already
          // decided.
          if (clockExpired || probeSignal.aborted) break;
          if (frame.kind !== "frame") break;

          if (frame.data === "[DONE]") {
            outcome = "SUCCESS";
            break;
          }
        }
      } else {
        const decision = decideProbeOutcome(response);
        outcome = decision.outcome;
        alert = decision.alert;
      }
    } catch (error) {
      const isAbortRejection = probeSignal.aborted && error === probeSignal.reason;

      // After the clock cancels the body, a pending read ends with `done`, not
      // with an error, so no failure reaches here once the clock decided.
      // The guard keeps the loser silent if an ordering ever delivers one.
      if (!clockExpired && !isAbortRejection) {
        caughtError = { value: error };
      }
    } finally {
      timeout.clear();
      timeout.signal.removeEventListener("abort", reportClockExpired);
      probeSignal.removeEventListener("abort", cancelReaderAfterAbort);

      try {
        vote(outcome);
      } finally {
        // Cancelling a reader the clock already cancelled changes nothing; the
        // guard states that the body was already released.
        if (reader !== undefined && !readerCancelledByAbort) {
          await cancelStream(reader, log, "breaker_probe").catch(() => {
            // Only the cancel-rejected log line can reject here; neither the
            // vote nor the diagnostic line depends on it.
          });
        }
      }
    }

    if (alert !== undefined) {
      try {
        emitOperationalAlert(log, alert, { req_id: PROBE_REQUEST_ID });
      } catch {
        // The probe has already voted, so a failed alert line must not turn
        // the attempt into an unhandled rejection. The alert's name is already
        // marked as fired, so it stays silent until the process restarts.
      }
    }

    if (caughtError !== undefined) {
      try {
        log.info({
          err_name: extractErrName(caughtError.value),
          ...extractCauseLog(caughtError.value),
        });
      } catch {
        // The probe has already voted. A diagnostic logger failure must not
        // turn that completed attempt into an unhandled rejection.
      }
    }
  };
}
