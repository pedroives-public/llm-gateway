import { describe, it, expect } from "vitest";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { createOpenAIClient } from "../../src/upstream/openai.js";
import type { Logger } from "../../src/upstream/rejection.js";

// Integration tests for the buffered upstream client against a real in-process
// HTTP/1.1 server (mirrors the runtime-pin convention — real sockets, not a
// mock — because the size-cap behavior depends on actual stream teardown).

const silentLog: Logger = { error: () => {} };
const freshSignal = (): AbortSignal => new AbortController().signal;
const reqBody = { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] };

async function withServer(
  handler: http.RequestListener,
  fn: (baseURL: string) => Promise<void>,
): Promise<void> {
  const server = http.createServer(handler);
  server.on("clientError", () => {});
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  try {
    await fn(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe("openai buffered client", () => {
  it("forwards a 200 JSON body as an ok Outcome", async () => {
    const body = { id: "chatcmpl-1", choices: [{ index: 0 }] };
    await withServer(
      (_req, res) => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(body));
      },
      async (baseURL) => {
        const client = createOpenAIClient({ apiKey: "k", baseURL });
        const outcome = await client.buffered(reqBody, freshSignal(), silentLog);
        expect(outcome).toEqual({ kind: "ok", status: 200, body_parsed: body });
      },
    );
  });

  it("forwards a non-2xx body verbatim as upstream_error", async () => {
    const errBody = '{"error":{"message":"upstream boom"}}';
    await withServer(
      (_req, res) => {
        res.writeHead(503, { "content-type": "application/json" });
        res.end(errBody);
      },
      async (baseURL) => {
        const client = createOpenAIClient({ apiKey: "k", baseURL });
        const outcome = await client.buffered(reqBody, freshSignal(), silentLog);
        expect(outcome).toEqual({
          kind: "upstream_error",
          status: 503,
          body_raw: errBody,
        });
      },
    );
  });

  it("treats a 200 with a non-JSON body as undecodable", async () => {
    // A 2xx proves the upstream executed, so an unparseable body is not retried —
    // it surfaces as `undecodable`, which the route normalizes to 502 upstream-fault.
    await withServer(
      (_req, res) => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{not valid json");
      },
      async (baseURL) => {
        const client = createOpenAIClient({ apiKey: "k", baseURL });
        const outcome = await client.buffered(reqBody, freshSignal(), silentLog);
        expect(outcome).toEqual({ kind: "undecodable" });
      },
    );
  });

  it("blocks an upstream redirect as redirect_blocked and never contacts the hop target", async () => {
    let targetHits = 0;
    await withServer(
      (_req, res) => {
        targetHits += 1;
        res.writeHead(204);
        res.end();
      },
      async (targetURL) => {
        await withServer(
          (_req, res) => {
            res.writeHead(302, { location: targetURL });
            res.end();
          },
          async (baseURL) => {
            const client = createOpenAIClient({ apiKey: "k", baseURL });
            const outcome = await client.buffered(
              reqBody,
              freshSignal(),
              silentLog,
            );
            expect(outcome).toEqual({ kind: "redirect_blocked" });
            // The egress defense itself: nothing reached the redirect target.
            expect(targetHits).toBe(0);
          },
        );
      },
    );
  });

  it(
    "caps a >1 MiB response as aborted{response_size_cap} and tears the upstream down",
    { timeout: 10_000 },
    async () => {
      const CHUNK = 64 * 1024;
      const chunk = Buffer.alloc(CHUNK, 0x61);
      const TARGET = 8 * 1024 * 1024; // 8 MiB — far above the 1 MiB cap + buffer window
      let serverBytes = 0;
      let serverReachedTarget = false;

      await withServer(
        (_req, res) => {
          res.on("error", () => {});
          res.writeHead(200, { "content-type": "application/json" });
          const pump = (): void => {
            if (res.destroyed || res.writableEnded) return;
            while (serverBytes < TARGET) {
              const ok = res.write(chunk);
              serverBytes += CHUNK;
              if (!ok) {
                res.once("drain", pump);
                return;
              }
            }
            serverReachedTarget = true;
            res.end();
          };
          pump();
        },
        async (baseURL) => {
          const client = createOpenAIClient({ apiKey: "k", baseURL });
          const outcome = await client.buffered(reqBody, freshSignal(), silentLog);
          expect(outcome).toEqual({
            kind: "aborted",
            abort_kind: "response_size_cap",
          });
          // §14: read-cancel tore the read down — the producer could not push the
          // full body. (That cancel halts the byte flow is pinned by the probe.)
          await new Promise((resolve) => setTimeout(resolve, 300));
          expect(serverReachedTarget).toBe(false);
          expect(serverBytes).toBeLessThan(TARGET);
        },
      );
    },
  );
});
