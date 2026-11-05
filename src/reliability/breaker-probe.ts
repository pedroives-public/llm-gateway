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
  type AlertName,
} from "../observability/events.js";
import { isInsufficientQuota } from "../upstream/quota.js";
import type { ErrorOutcome } from "../upstream/outcome.js";

const PROBE_REQUEST_ID = "breaker_probe";

type ProbeDecision = {
  outcome: ProbeOutcome;
  alert?: AlertName;
};

function decideProbeOutcome(response: ErrorOutcome): ProbeDecision {
  if (response.kind === "redirect_blocked") {
    return {
      outcome: "FAILURE",
      alert: "upstream_redirect_blocked",
    };
  }

  if (response.kind !== "upstream_error") {
    return { outcome: "FAILURE" };
  }

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
    return { outcome: "FAILURE", alert: "upstream_probe_rejected" };
  }

  return { outcome: "FAILURE" };
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
}: ProbeRunnerDependencies): (vote: ProbeVote) => Promise<void> {
  const requestBody: ProbeRequestBody = {
    model,
    messages: [{ role: "user", content: "ping" }],
    stream: true,
    max_tokens: 1,
  };

  return async (vote) => {
    const timeout = armWallClockTimeout(PROBE_CLOCK_MS);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let outcome: ProbeOutcome = "FAILURE";
    let alert: AlertName | undefined;
    let caughtError: { value: unknown } | undefined;

    try {
      const response = await streaming(requestBody, timeout.signal, log);

      if (isAcceptedStream(response)) {
        reader = response.reader;
        const nextFrame = createSseFrameReader(reader);

        while (true) {
          const frame = await nextFrame();

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
      caughtError = { value: error };
    } finally {
      timeout.clear();

      try {
        vote(outcome);
      } finally {
        if (reader !== undefined) {
          await cancelStream(reader, log, "breaker_probe");
        }
      }
    }

    if (alert !== undefined) {
      emitOperationalAlert(log, alert, { req_id: PROBE_REQUEST_ID });
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
