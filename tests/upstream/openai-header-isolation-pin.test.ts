import { describe, it, expect } from "vitest";
import http from "node:http";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import type { Logger } from "../../src/upstream/rejection.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";

// Header-isolation pin: the upstream credential, auth headers, and endpoint
// are deployment-owned (boot config); consumer input is data, never control.
// The 401 → breaker-delta-1 rule is authorized ONLY while this holds — if a
// consumer could influence the auth context, it could induce 401s and open
// the shared breaker for everyone. This pin MUST run the real adapter against
// a real socket: a fake-upstream seam bypasses the adapter and proves nothing
// about the headers it constructs.

const silentLog: Logger = { error: () => {} };
const freshSignal = (): AbortSignal => new AbortController().signal;

// Unique token planted in every consumer-controlled field; found anywhere
// outside the forwarded body, consumer data crossed into control.
const MARKER = "consumer-controlled-marker-6f2b";

type CapturedRequest = {
  headers: http.IncomingHttpHeaders;
  url: string;
  body: string;
};

describe("openai client header isolation (deployment-owned auth context)", () => {
  it("consumer body fields cannot influence the authorization header or the endpoint", async () => {
    let captured: CapturedRequest | null = null;

    const server = http.createServer((req, res) => {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString("utf8");
      });
      req.on("end", () => {
        captured = { headers: req.headers, url: req.url ?? "", body };
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "chatcmpl-pin" }));
      });
    });
    const { port, close } = await listenEphemeral(server);

    try {
      const client = createOpenAIClient({
        apiKey: "deployment-key",
        baseURL: `http://127.0.0.1:${port}`,
      });

      // Hostile-but-schema-plausible body: every consumer-reachable field
      // carries the marker, including keys that LOOK like auth/routing config.
      const hostileBody = {
        model: `gpt-4o/../${MARKER}`,
        messages: [{ role: "user", content: MARKER }],
        authorization: `Bearer ${MARKER}`,
        api_key: MARKER,
        baseURL: `http://${MARKER}.invalid`,
        headers: { authorization: `Bearer ${MARKER}` },
      };

      const outcome = await client.buffered(
        hostileBody,
        freshSignal(),
        silentLog,
      );
      expect(outcome.kind).toBe("ok");

      if (captured === null) {
        throw new Error("upstream server captured no request");
      }
      const seen: CapturedRequest = captured;

      // Credential comes from deployment config alone, byte-exact.
      expect(seen.headers.authorization).toBe("Bearer deployment-key");

      // Endpoint path is fixed by the adapter, never derived from the body.
      expect(seen.url).toBe("/chat/completions");

      // No request header (name or value) carries consumer-controlled bytes.
      for (const [name, value] of Object.entries(seen.headers)) {
        const flat = Array.isArray(value) ? value.join(",") : (value ?? "");
        expect(`${name}: ${flat}`).not.toContain(MARKER);
      }

      // The same bytes DO reach the upstream inside the body, verbatim —
      // proving the boundary splits data from control, not that it drops data.
      expect(seen.body).toContain(MARKER);
    } finally {
      await close();
    }
  });
});
