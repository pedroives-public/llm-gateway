import http from "node:http";
import { describe, expect, it } from "vitest";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { stubBreaker } from "../helpers/breaker-stubs.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { bearer } from "../helpers/fake-auth-db.js";
import { buildProxyApp } from "../helpers/proxy-app.js";

// Wire pin for the output limit: the exact text the upstream receives, through
// the real route and the real adapter against a real socket. The cells in
// proxy-cost-caps.test.ts and proxy-streaming-branch.test.ts check the object
// the route hands to the adapter. This file checks what the adapter then
// writes, so a field added, renamed or dropped after that handoff shows here.
//
// The rule: the upstream is sent one output-limit field,
// max_completion_tokens, never max_tokens. Its value is the client's
// max_completion_tokens, else the client's max_tokens, else the cap of 16384.
// Every other field keeps the place and the text the client gave it.
//
// Both columns are literal text. The client column is sent as written, so the
// order of the fields and the quoted "100" of the last row are part of the
// input, not a product of a serializer.

type WirePath = "buffered" | "streaming";

type WireRow = {
  path: WirePath;
  name: string;
  clientBody: string;
  wireBody: string;
  because: string;
};

const WIRE_ROWS: WireRow[] = [
  {
    path: "buffered",
    name: "no output limit",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":16384}',
    because:
      "a request with no output limit reaches the upstream with the cap as max_completion_tokens and no other limit field",
  },
  {
    path: "buffered",
    name: "max_tokens",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_tokens":100}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":100}',
    because:
      "a client's max_tokens reaches the upstream as max_completion_tokens with the same value, and the max_tokens name is not sent",
  },
  {
    path: "buffered",
    name: "max_completion_tokens",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":100}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":100}',
    because:
      "a client's max_completion_tokens reaches the upstream unchanged",
  },
  {
    path: "buffered",
    name: "both fields",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"max_completion_tokens":200}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":200}',
    because:
      "when a client sends both fields, only max_completion_tokens is sent, with the client's max_completion_tokens value",
  },
  {
    path: "buffered",
    name: "max_tokens as a JSON string",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_tokens":"100"}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":100}',
    because:
      "a max_tokens sent as a JSON string reaches the upstream as a JSON number under max_completion_tokens",
  },
  {
    path: "streaming",
    name: "no output limit",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_completion_tokens":16384}',
    because:
      "a request with no output limit reaches the upstream with the cap as max_completion_tokens and no other limit field",
  },
  {
    path: "streaming",
    name: "max_tokens",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":100}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_completion_tokens":100}',
    because:
      "a client's max_tokens reaches the upstream as max_completion_tokens with the same value, and the max_tokens name is not sent",
  },
  {
    path: "streaming",
    name: "max_completion_tokens",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_completion_tokens":100}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_completion_tokens":100}',
    because:
      "a client's max_completion_tokens reaches the upstream unchanged",
  },
  {
    path: "streaming",
    name: "both fields",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":100,"max_completion_tokens":200}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_completion_tokens":200}',
    because:
      "when a client sends both fields, only max_completion_tokens is sent, with the client's max_completion_tokens value",
  },
  {
    path: "streaming",
    name: "max_tokens as a JSON string",
    clientBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":"100"}',
    wireBody:
      '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"max_completion_tokens":100}',
    because:
      "a max_tokens sent as a JSON string reaches the upstream as a JSON number under max_completion_tokens",
  },
];

describe("output limit on the wire (real route, real adapter, real socket)", () => {
  it.each(WIRE_ROWS)("$path, $name", async (row) => {
    // Fake upstream: keeps the text of every request body it receives and
    // answers the way a provider answers that path, so the route completes.
    const receivedBodies: string[] = [];
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => {
        chunks.push(chunk);
      });
      req.on("end", () => {
        receivedBodies.push(Buffer.concat(chunks).toString("utf8"));
        if (row.path === "streaming") {
          res.writeHead(200, { "content-type": "text/event-stream" });
          res.end("data: [DONE]\n\n");
        } else {
          res.writeHead(200, { "content-type": "application/json" });
          res.end('{"id":"chatcmpl-wire"}');
        }
      });
    });
    const { port, close } = await listenEphemeral(server);

    const client = createOpenAIClient({
      apiKey: "deployment-key",
      baseURL: `http://127.0.0.1:${port}`,
    });
    const app = await buildProxyApp({
      breaker: stubBreaker,
      streamingEnabled: true,
      upstreamBuffered: client.buffered,
      upstreamStreaming: client.streaming,
    });

    try {
      const res = await app.inject({
        method: "POST",
        url: "/v1/chat/completions",
        headers: {
          authorization: bearer(),
          "content-type": "application/json",
        },
        payload: row.clientBody,
      });

      expect(
        res.statusCode,
        "control: the gateway accepted the request and answered it",
      ).toBe(200);
      expect(
        receivedBodies,
        "control: exactly one request reached the upstream",
      ).toHaveLength(1);
      expect(receivedBodies[0], row.because).toBe(row.wireBody);
    } finally {
      await app.close();
      await close();
    }
  });
});
