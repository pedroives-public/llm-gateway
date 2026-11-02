import type { StreamingUpstream } from "../upstream/openai.js";
import { isAcceptedStream } from "../upstream/stream.js";
import { createSseFrameReader } from "../upstream/sse-frame-reader.js";
import type { ProbeOutcome, ProbeVote } from "./circuit-breaker.js";
import { extractCauseLog } from "../upstream/cause-log.js";
import { extractErrName } from "../upstream/rejection.js";

type ProbeRunnerDependencies = {
  streaming: StreamingUpstream;
  model: string;
  log: Parameters<StreamingUpstream>[2];
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
    const controller = new AbortController();
    const signal = controller.signal;

    let outcome: ProbeOutcome = "FAILURE";
    let caughtError: { value: unknown } | undefined;

    try {
      const response = await streaming(requestBody, signal, log);

      if (isAcceptedStream(response)) {
        const nextFrame = createSseFrameReader(response.reader);

        while (true) {
          const frame = await nextFrame();

          if (frame.kind !== "frame") break;

          if (frame.data === "[DONE]") {
            outcome = "SUCCESS";
            break;
          }
        }
      }
    } catch (error) {
      caughtError = { value: error };
    }

    vote(outcome);

    if (caughtError !== undefined) {
      log.info({
        err_name: extractErrName(caughtError.value),
        ...extractCauseLog(caughtError.value),
      });
    }
  };
}
