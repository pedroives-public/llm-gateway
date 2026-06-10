import { recognize } from "../../src/upstream/recognize.js";
import { describe, it, expect } from "vitest";

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

    expect(result).toEqual({ kind: "undecodable" });
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
