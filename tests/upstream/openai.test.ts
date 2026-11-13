import { afterEach, describe, it, expect, vi } from "vitest";
import http from "node:http";
import type { AddressInfo } from "node:net";
import {
  createOpenAIClient,
  type OpenAIClient,
} from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import {
  UPSTREAM_CLOSE_DEADLINE_MS,
  withinDeadline,
} from "../helpers/streaming-seams.js";

// Integration tests for the buffered upstream client against a real in-process
// HTTP/1.1 server (mirrors the runtime-pin convention — real sockets, not a
// mock — because the size-cap behavior depends on actual stream teardown).

const silentLog = { error: () => {}, info: () => {} };
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
        expect(
          outcome,
          "a 200 whose body is not JSON reaches the buffered recognizer as the unparsable_body fact, never as a fact about the head",
        ).toEqual({
          kind: "undecodable",
          undecodable_kind: "unparsable_body",
        });
      },
    );
  });

  it("returns a 302 as the 3xx outcome and never contacts the hop target", async () => {
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
            expect(
              outcome,
              "a 302 reaches the adapter as a response and becomes the 3xx outcome: the runtime must neither reject it nor follow it",
            ).toStrictEqual({ kind: "upstream_3xx", status: 302 });
            // The egress defense itself: nothing reached the redirect target.
            expect(
              targetHits,
              "nothing may reach the host that Location names",
            ).toBe(0);
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

type HeldUpstream = {
  baseURL: string;
  // Resolves with the response's `writableFinished` when its connection
  // closes: `false` means the connection died before the fake ended the body.
  closed: Promise<boolean>;
  close: () => Promise<void>;
};

// A fake upstream that sends the given head and the first bytes of a body,
// then never ends it. Only the adapter releasing its read can close the
// connection, so the close is evidence of the release; a fake that ended the
// body would report the same close with or without one.
async function holdOpenUpstream(
  status: number,
  firstBytes: string,
): Promise<HeldUpstream> {
  let reportClose: (finished: boolean) => void = () => {};
  const closed = new Promise<boolean>((resolve) => {
    reportClose = resolve;
  });

  const server = http.createServer((req, res) => {
    res.on("error", () => {});
    res.on("close", () => {
      reportClose(res.writableFinished);
    });
    req.resume();
    req.on("end", () => {
      res.writeHead(status, { "content-type": "text/plain" });
      res.write(firstBytes);
    });
  });
  const { port, close } = await listenEphemeral(server);

  return {
    baseURL: `http://127.0.0.1:${port}`,
    closed,
    close: async () => {
      // A cell that fails leaves the held response open; close() alone would
      // wait for it forever.
      server.closeAllConnections();
      await close();
    },
  };
}

type AdapterLog = Parameters<OpenAIClient["buffered"]>[2];

// The two calls of the adapter, driven the same way: every custody rule of a
// 3xx body holds on both.
const adapterCalls = [
  {
    call: "buffered",
    run: (
      client: OpenAIClient,
      signal: AbortSignal,
      log: AdapterLog,
    ): Promise<unknown> => client.buffered(reqBody, signal, log),
  },
  {
    call: "streaming",
    run: (
      client: OpenAIClient,
      signal: AbortSignal,
      log: AdapterLog,
    ): Promise<unknown> =>
      client.streaming({ ...reqBody, stream: true }, signal, log),
  },
];

describe("openai client: custody of a 3xx body", () => {
  it.each(adapterCalls)(
    "$call call: a 300 whose body stays open is decided at the head and its body is released",
    async ({ run }) => {
      const upstream = await holdOpenUpstream(300, "held");
      const caller = new AbortController();
      try {
        const client = createOpenAIClient({
          apiKey: "k",
          baseURL: upstream.baseURL,
        });

        const outcome = await withinDeadline(
          run(client, caller.signal, silentLog),
          UPSTREAM_CLOSE_DEADLINE_MS,
          "the adapter did not decide the 3xx at the response head: no outcome before the deadline while the body was open",
        );
        expect(outcome).toStrictEqual({ kind: "upstream_3xx", status: 300 });

        const finished = await withinDeadline(
          upstream.closed,
          UPSTREAM_CLOSE_DEADLINE_MS,
          "the 3xx body was not released: the fake saw no close before the deadline",
        );
        expect(
          finished,
          "the upstream connection closed only after the fake ended the body, not by the adapter releasing it",
        ).toBe(false);
      } finally {
        // A cell that fails leaves the adapter's read pending; the caller's
        // abort ends it.
        caller.abort();
        await upstream.close();
      }
    },
  );

  it.each(adapterCalls)(
    "$call call: a 304, which carries no body, is the 3xx outcome and nothing is released",
    async ({ run }) => {
      await withServer(
        (_req, res) => {
          res.writeHead(304);
          res.end();
        },
        async (baseURL) => {
          const client = createOpenAIClient({ apiKey: "k", baseURL });

          const settled = await run(client, freshSignal(), silentLog).then(
            (outcome) => outcome,
            (error: unknown) => `rejected: ${String(error)}`,
          );

          expect(
            settled,
            "a 3xx without a body is decided at the head and has nothing to release: the adapter must not ask a missing body for a reader",
          ).toStrictEqual({ kind: "upstream_3xx", status: 304 });
        },
      );
    },
  );
});

class AbortLikeError extends Error {
  override name = "AbortError";
}

describe("openai client: a 3xx body whose cancel rejects", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each(adapterCalls)(
    "$call call: the rejected cancel is one log line and the 3xx outcome is kept",
    async ({ run }) => {
      // The runtime's fetch rejects a cancel only on a body that already
      // errored, so the response reaches the adapter through a wrapper around
      // the global fetch: a real ReadableStream whose source rejects its
      // cancel.
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          return Promise.reject(new AbortLikeError("SECRET-IN-MESSAGE"));
        },
      });
      vi.stubGlobal("fetch", () =>
        Promise.resolve(new Response(body, { status: 300 })),
      );
      const lines: object[] = [];
      const log: AdapterLog = {
        error: () => {},
        info: (line) => {
          lines.push(line);
        },
      };
      const client = createOpenAIClient({
        apiKey: "k",
        baseURL: "http://upstream.invalid",
      });

      // The stubbed body never ends, so an adapter that waited for it would
      // hang: the deadline turns that into a named failure.
      const settled = await withinDeadline(
        run(client, freshSignal(), log).then(
          (outcome) => outcome,
          (error: unknown) => `rejected: ${String(error)}`,
        ),
        UPSTREAM_CLOSE_DEADLINE_MS,
        "the adapter did not decide the 3xx at the response head: no outcome before the deadline while the body was open",
      );

      expect(
        settled,
        "a cancel that rejects is a cleanup observation: it must not replace the 3xx outcome",
      ).toStrictEqual({ kind: "upstream_3xx", status: 300 });
      expect(
        lines,
        "a rejected cancel of a 3xx body writes exactly one line, from the adapter's site, carrying the error's name and never its message",
      ).toStrictEqual([
        {
          event: "upstream_cancel_rejected",
          site: "upstream_adapter",
          err_name: "AbortError",
        },
      ]);
    },
  );
});

describe("openai client: the redirect option handed to the runtime", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each(adapterCalls)(
    '$call call: sends exactly one request, with redirect "manual"',
    async ({ run }) => {
      // The option is written inside the adapter and is not a setting, so the
      // cell reads it at the point where the adapter hands the request to the
      // runtime: a wrapper around the global fetch that records each call. It
      // answers a 304, which the adapter decides at the head without a body.
      const sentRedirects: Array<RequestInit["redirect"]> = [];
      vi.stubGlobal("fetch", (_input: unknown, init?: RequestInit) => {
        sentRedirects.push(init?.redirect);
        return Promise.resolve(new Response(null, { status: 304 }));
      });
      const client = createOpenAIClient({
        apiKey: "k",
        baseURL: "http://upstream.invalid",
      });

      await run(client, freshSignal(), silentLog);

      expect(
        sentRedirects,
        'the adapter must hand the runtime exactly one request carrying redirect "manual": the runtime then returns a 3xx as a response for the adapter to decide and never follows it',
      ).toStrictEqual(["manual"]);
    },
  );
});
