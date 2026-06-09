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
});
