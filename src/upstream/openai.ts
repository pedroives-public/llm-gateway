import type { Outcome } from "./outcome.js";
import {
  recognizeResponse,
  type ResponseFacts,
} from "./recognize.js";
import {
  resolveRejection,
  type Logger,
} from "./rejection.js";
import type { StreamingAdapter } from "./stream.js";
import type { MinLogger } from "../observability/events.js";
import { cancelStream } from "./stream.js";

// OpenAI-compatible buffered client. Routes the fetch result through the §2
// recognition pipeline so the route gets a typed Outcome, never a raw throw.
// The per-call logger lets resolveRejection's §2 boundary log stay attributable
// to the request.

export interface OpenAIClientConfig {
  apiKey: string;
  baseURL: string;
}

export type BufferedUpstream = (
  body: unknown,
  signal: AbortSignal,
  log: Logger,
) => Promise<Outcome>;

export type StreamingUpstream = (
  body: unknown,
  signal: AbortSignal,
  log: Logger & MinLogger,
) => Promise<StreamingAdapter>;

export interface OpenAIClient {
  buffered: BufferedUpstream;
  streaming: StreamingUpstream;
}

type CappedRead = { capped: true } | { capped: false; bodyText: string };

export function createOpenAIClient(config: OpenAIClientConfig): OpenAIClient {
  const endpoint = `${config.baseURL}/chat/completions`;

  const fetchUpstream = (body: unknown, signal: AbortSignal) =>
    fetch(endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${config.apiKey}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
      // Fail-closed egress: a 3xx from the configured endpoint is an
      // operator-config fault; following it would replay the request
      // (tenant body included) against an unvetted host.
      redirect: "error",
      signal,
    });

  const buffered: BufferedUpstream = async (body, signal, log) => {
    let response: Response;
    try {
      response = await fetchUpstream(body, signal);
    } catch (err) {
      // resolveRejection owns rejections: network/abort → Outcome, else re-throw.
      return resolveRejection(err, signal, log);
    }

    let read: CappedRead;
    try {
      read = await readBodyWithCap(response);
    } catch (err) {
      // A mid-body read rejection (e.g. the wall-clock abort) is the same boundary.
      return resolveRejection(err, signal, log);
    }

    if (read.capped) {
      // Synthesized terminal, not a rejection: the client stopped its own read.
      return { kind: "aborted", abort_kind: "response_size_cap" };
    }

    const retryAfter = response.headers.get("retry-after") ?? undefined;
    return recognizeResponse(
      toResponseFacts(response.status, read.bodyText, retryAfter),
    );
  };

  const streaming: StreamingUpstream = async (body, signal, log) => {
    let response: Response;
    try {
      response = await fetchUpstream(body, signal);
    } catch (err) {
      // resolveRejection owns rejections: network/abort → Outcome, else re-throw.
      return resolveRejection(err, signal, log);
    }

    if (response.status < 200 || response.status >= 300) {
      let read: CappedRead;
      try {
        read = await readBodyWithCap(response);
      } catch (err) {
        // A mid-body read rejection (e.g. the wall-clock abort) is the same boundary.
        return resolveRejection(err, signal, log);
      }

      if (read.capped) {
        // Synthesized terminal, not a rejection: the client stopped its own read.
        return { kind: "aborted", abort_kind: "response_size_cap" };
      }

      const retryAfter = response.headers.get("retry-after") ?? undefined;
      const recognized = recognizeResponse(
        toResponseFacts(response.status, read.bodyText, retryAfter),
      );

      if (recognized.kind !== "upstream_error") {
        throw new Error(
          "Every non-2xx response must be recognized as an upstream_error",
        );
      }

      return recognized;
    }

    const contentType = response.headers.get("content-type");
    const mediaType = contentType?.split(";", 1)[0]?.trim().toLowerCase();

    if (mediaType !== "text/event-stream" || response.body === null) {
      if (response.body !== null) {
        const reader = response.body.getReader();
        await cancelStream(reader, log, "upstream_adapter");
      }

      return { kind: "undecodable" };
    }

    return {
      kind: "accepted_stream",
      status: response.status,
      reader: response.body.getReader(),
    };
  };

  return { buffered, streaming };
}

// Parse only a 2xx; a non-2xx is forwarded verbatim as body_raw, and a 2xx that
// fails to parse is undecodable — neither is a rejection.
function toResponseFacts(
  status: number,
  bodyText: string,
  retryAfter: string | undefined,
): ResponseFacts {
  if (status >= 200 && status < 300) {
    try {
      return {
        resolved: true,
        parsed: true,
        status,
        body_parsed: JSON.parse(bodyText) as unknown,
      };
    } catch {
      return { resolved: true, parsed: false, status, body_raw: bodyText };
    }
  }
  return {
    resolved: true,
    parsed: false,
    status,
    body_raw: bodyText,
    retry_after: retryAfter,
  };
}

const RESPONSE_SIZE_CAP_BYTES = 1024 * 1024; // 1 MiB — §14 self-inflicted-DoS defense.

async function readBodyWithCap(response: Response): Promise<CappedRead> {
  if (response.body === null) {
    // No body to read (e.g. a 204). Nothing to cap; decode of empty is "".
    return { capped: false, bodyText: "" };
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let bodyText = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    total += value.length;
    // strict: spec rejects bodies "larger than" the cap, so exactly-cap passes
    if (total > RESPONSE_SIZE_CAP_BYTES) {
      await reader.cancel();
      return { capped: true };
    }

    bodyText += decoder.decode(value, { stream: true });
  }

  // Stream ended cleanly under the cap. Flush any bytes the streaming decoder
  // was holding (a multi-byte char split across the last chunk boundary).
  bodyText += decoder.decode();
  return { capped: false, bodyText };
}
