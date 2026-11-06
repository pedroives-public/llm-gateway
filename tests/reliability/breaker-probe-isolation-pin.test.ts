import { describe, expect, it, vi } from "vitest";
import http from "node:http";
import { createProbeRunner } from "../../src/reliability/breaker-probe.js";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";

// The recovery probe decides for every tenant, so its request must be built
// from deployment config alone. This pin runs the real adapter against a real
// socket: a fake seam would prove nothing about the bytes that leave the
// gateway. A tenant request goes first through the same client, because the
// adapter takes no headers and no consumer key, which leaves state kept between
// calls as the only way tenant input could reach a probe at this boundary.

const CANARY = "tenant-canary-6f2b";

// Names added by the deployment-owned adapter and by the runtime's fetch. A
// new name means someone added a header to the request the upstream receives.
const ALLOWED_HEADER_NAMES = new Set([
  "accept",
  "accept-encoding",
  "accept-language",
  "authorization",
  "connection",
  "content-length",
  "content-type",
  "host",
  "sec-fetch-mode",
  "user-agent",
]);

type CapturedRequest = {
  method: string;
  url: string;
  headers: http.IncomingHttpHeaders;
  body: string;
};

const silentLog = { info: () => {}, error: () => {} };

// The app is not closing in these scenes: a signal that never aborts.
const NOT_CLOSING = new AbortController().signal;

describe("breaker probe isolation (deployment-owned request)", () => {
  it("sends only deployment config after a hostile tenant request", async () => {
    let captured: CapturedRequest[] = [];
    const server = http.createServer((req, res) => {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString("utf8");
      });
      req.on("end", () => {
        captured = [
          ...captured,
          {
            method: req.method ?? "",
            url: req.url ?? "",
            headers: req.headers,
            body,
          },
        ];
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end("data: [DONE]\n\n");
      });
    });
    const { port, close } = await listenEphemeral(server);

    try {
      const client = createOpenAIClient({
        apiKey: "deployment-key",
        baseURL: `http://127.0.0.1:${port}`,
      });

      const tenantResponse = await client.streaming(
        {
          model: `gpt-4o/../${CANARY}`,
          messages: [{ role: "user", content: CANARY }],
          stream: true,
          authorization: `Bearer ${CANARY}`,
          api_key: CANARY,
          baseURL: `http://${CANARY}.invalid`,
          headers: { authorization: `Bearer ${CANARY}` },
        },
        new AbortController().signal,
        silentLog,
      );
      if (tenantResponse.kind === "accepted_stream") {
        await tenantResponse.reader.cancel();
      }

      const vote = vi.fn();
      const runProbe = createProbeRunner({
        streaming: client.streaming,
        model: "deployment-probe-model",
        log: silentLog,
      });
      await runProbe(vote, NOT_CLOSING);

      expect(
        captured.length,
        "the scene needs exactly one tenant request and one probe",
      ).toBe(2);
      const [tenant, probe] = captured;
      if (tenant === undefined || probe === undefined) {
        throw new Error("the upstream captured fewer than two requests");
      }

      // Positive control: the canary does reach this server, so its absence
      // from the probe is a reading and not a blind ruler.
      expect(
        tenant.body,
        "the tenant request must carry the canary, or the ruler below sees nothing",
      ).toContain(CANARY);

      expect(vote, "the probe must run through the real adapter").toHaveBeenCalledWith(
        "SUCCESS",
      );
      expect(probe.method, "the probe is an inference request").toBe("POST");
      expect(
        probe.url,
        "the probe's endpoint comes from deployment config alone",
      ).toBe("/chat/completions");
      expect(
        probe.headers.authorization,
        "the probe's credential comes from deployment config alone",
      ).toBe("Bearer deployment-key");
      expect(
        JSON.parse(probe.body),
        "the probe's body is fixed: the configured model and nothing from a tenant",
      ).toEqual({
        model: "deployment-probe-model",
        messages: [{ role: "user", content: "ping" }],
        stream: true,
        max_tokens: 1,
      });
      expect(
        Object.keys(probe.headers).filter(
          (name) => !ALLOWED_HEADER_NAMES.has(name),
        ),
        "the probe carries only the headers the adapter and the runtime add",
      ).toEqual([]);

      // Explicit net over what the literals above do not fix: the values of
      // the runtime's own headers.
      expect(
        JSON.stringify(probe),
        "no tenant bytes may reach the probe's request",
      ).not.toContain(CANARY);
    } finally {
      await close();
    }
  });
});
