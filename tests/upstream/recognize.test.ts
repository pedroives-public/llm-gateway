import {
  recognize,
  recognizeRejection,
  recognizeResponse,
} from "../../src/upstream/recognize.js";
import type { UnrecognizedRejection } from "../../src/upstream/recognize.js";
import type { Outcome, ErrorOutcome } from "../../src/upstream/outcome.js";
import { describe, it, expect, expectTypeOf } from "vitest";

describe("recognize", () => {
  it("maps a resolved non-2xx response to upstream_error carrying status and raw body", () => {
    const result = recognize({
      resolved: true,
      parsed: false,
      status: 500,
      body_raw: '{"error":"upstream boom"}',
    });

    expect(result).toEqual({
      kind: "upstream_error",
      status: 500,
      body_raw: '{"error":"upstream boom"}',
    });
  });

  it("maps a resolved 4xx the same way as a 5xx — any non-2xx is upstream_error", () => {
    const result = recognize({
      resolved: true,
      parsed: false,
      status: 400,
      body_raw: '{"error":"bad request"}',
    });

    expect(result).toEqual({
      kind: "upstream_error",
      status: 400,
      body_raw: '{"error":"bad request"}',
    });
  });

  // The gateway recognizes a 3xx by its status alone, whatever its body says.
  const EVERY_3XX_STATUS = Array.from({ length: 100 }, (_, index) => 300 + index);

  it("lists every status from 300 to 399 exactly once (control of the table below)", () => {
    expect(EVERY_3XX_STATUS).toHaveLength(100);
    expect(EVERY_3XX_STATUS[0]).toBe(300);
    expect(EVERY_3XX_STATUS[99]).toBe(399);
  });

  it.each(EVERY_3XX_STATUS)(
    "recognizes status %i by the status alone, as the 3xx outcome carrying it",
    (status) => {
      const result = recognize({
        resolved: true,
        parsed: false,
        status,
        body_raw: "a body the recognition must not depend on",
      });

      expect(
        result,
        "a 3xx is the upstream's own answer and is recognized by its status: never an upstream_error",
      ).toEqual({ kind: "upstream_3xx", status });
    },
  );

  it("keeps 299 a success and 400 an upstream_error: the 3xx range ends at its own edges", () => {
    expect(
      recognize({ resolved: true, parsed: true, status: 299, body_parsed: { id: "chatcmpl-1" } }),
      "299 is the last 2xx: a body that parses is ok, never the 3xx outcome",
    ).toEqual({ kind: "ok", status: 299, body_parsed: { id: "chatcmpl-1" } });

    expect(
      recognize({ resolved: true, parsed: false, status: 400, body_raw: '{"error":"bad request"}' }),
      "400 is the first 4xx: an upstream_error, never the 3xx outcome",
    ).toEqual({ kind: "upstream_error", status: 400, body_raw: '{"error":"bad request"}' });
  });

  it("maps a resolved 2xx response with a parsed body to ok", () => {
    const result = recognize({
      resolved: true,
      parsed: true,
      status: 200,
      body_parsed: { id: "chatcmpl-1", object: "chat.completion" },
    });

    expect(result).toEqual({
      kind: "ok",
      status: 200,
      body_parsed: { id: "chatcmpl-1", object: "chat.completion" },
    });
  });

  it("maps a resolved 2xx whose body failed to parse to undecodable (body discarded)", () => {
    const result = recognize({
      resolved: true,
      parsed: false,
      status: 200,
      body_raw: "<html>200 OK but not JSON</html>",
    });

    expect(
      result,
      "a resolved 2xx whose body failed to parse is the unparsable_body fact, the only one the buffered recognizer reports",
    ).toEqual({
      kind: "undecodable",
      undecodable_kind: "unparsable_body",
    });
  });

  it.each(["ENOTFOUND", "ECONNREFUSED", "UND_ERR_CONNECT_TIMEOUT"])(
    "maps the pre-send-proven connection failure %s to network_failed { pre_send_proven: true }",
    (causeCode) => {
      const result = recognize({
        resolved: false,
        rejection: "network",
        cause_code: causeCode,
      });

      expect(result).toEqual({
        kind: "network_failed",
        pre_send_proven: true,
      });
    },
  );

  it.each([
    "ECONNRESET",
    "ETIMEDOUT",
    "UND_ERR_HEADERS_TIMEOUT",
    "UND_ERR_BODY_TIMEOUT",
    "EPIPE",
    "UND_ERR_SOCKET",
  ])(
    "maps the possibly-post-send connection failure %s to network_failed { pre_send_proven: false }",
    (causeCode) => {
      const result = recognize({
        resolved: false,
        rejection: "network",
        cause_code: causeCode,
      });

      expect(result).toEqual({
        kind: "network_failed",
        pre_send_proven: false,
      });
    },
  );

  it("returns the unrecognized variant AS A VALUE for a cause code on neither list (total — never throws)", () => {
    const result = recognize({
      resolved: false,
      rejection: "network",
      cause_code: "EHOSTUNREACH",
    });

    expect(result).toEqual({ kind: "unrecognized" });
  });

  it.each(["response_size_cap", "wall_clock_expired", "total_timeout"] as const)(
    "maps the recognized abort kind %s to aborted { abort_kind }",
    (abortKind) => {
      const result = recognize({
        resolved: false,
        rejection: "abort",
        abort_kind: abortKind,
      });

      expect(result).toEqual({ kind: "aborted", abort_kind: abortKind });
    },
  );

  it("returns the unrecognized variant for an abort kind outside the enum (total — never throws)", () => {
    const result = recognize({
      resolved: false,
      rejection: "abort",
      abort_kind: "rogue_kind",
    });

    expect(result).toEqual({ kind: "unrecognized" });
  });

  it("keeps the unrecognized variant outside Outcome, so recognize's return cannot reach classify without narrowing", () => {
    expectTypeOf<UnrecognizedRejection>().not.toExtend<Outcome>();
    expectTypeOf<ReturnType<typeof recognize>>().not.toExtend<ErrorOutcome>();
  });

  it("exposes branch-specific recognizers with branch-specific return types", () => {
    expectTypeOf<ReturnType<typeof recognizeRejection>>().toEqualTypeOf<
      | Extract<Outcome, { kind: "network_failed" | "aborted" }>
      | UnrecognizedRejection
    >();
    expectTypeOf<ReturnType<typeof recognizeResponse>>().toEqualTypeOf<
      Extract<Outcome, { kind: "ok" | "upstream_error" | "undecodable" | "upstream_3xx" }>
    >();
  });

  it("carries a raw Retry-After header onto upstream_error for a 429", () => {
    const result = recognize({
      resolved: true,
      parsed: false,
      status: 429,
      body_raw: '{"error":"rate limited"}',
      retry_after: "120",
    });

    expect(result).toEqual({
      kind: "upstream_error",
      status: 429,
      body_raw: '{"error":"rate limited"}',
      retry_after: "120",
    });
  });
});
