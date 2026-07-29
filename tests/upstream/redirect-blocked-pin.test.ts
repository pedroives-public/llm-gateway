import { describe, it, expect } from "vitest";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { REDIRECT_BLOCKED_MESSAGE } from "../../src/upstream/recognize.js";

// ENVIRONMENT PIN — the Node/undici behavior the redirect discriminator
// rests on. The recognizer keys on undici's redirect rejection shape (free
// text, not a contract): cause.message plus the ABSENCE of cause.code. If a
// Node bump changes either half, this file must fail loud before a redirect
// silently falls back to the unrecognized 500 lane in production.
//
// The follow control proves the apparatus: same fixture, only `redirect`
// changes. Without it, a rejection here could be the fixture failing rather
// than the policy acting — the two runs are only attributable together.

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

// Target + redirector pair; `targetHits` is the positive oracle (a followed
// hop is a counted connection, not an inferred one).
async function withRedirectPair(
  fn: (redirectorURL: string, targetHits: () => number) => Promise<void>,
): Promise<void> {
  let hits = 0;
  await withServer(
    (_req, res) => {
      hits += 1;
      res.writeHead(204);
      res.end();
    },
    async (targetURL) => {
      await withServer(
        (_req, res) => {
          res.writeHead(302, { location: targetURL });
          res.end();
        },
        async (redirectorURL) => {
          await fn(redirectorURL, () => hits);
        },
      );
    },
  );
}

// Mirrors the production call shape (src/upstream/openai.ts); only `redirect`
// is the variable under test.
function attempt(url: string, redirect: "error" | "follow"): Promise<Response> {
  return fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ probe: true }),
    redirect,
  });
}

async function rejectionOf(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (err) {
    return err;
  }
  throw new Error("expected the fetch to reject, but it resolved");
}

function causeOf(err: unknown): unknown {
  if (typeof err === "object" && err !== null && "cause" in err) {
    return err.cause;
  }
  return undefined;
}

function messageOf(x: unknown): string | undefined {
  if (
    typeof x === "object" &&
    x !== null &&
    "message" in x &&
    typeof x.message === "string"
  ) {
    return x.message;
  }
  return undefined;
}

function codeOf(x: unknown): string | undefined {
  if (
    typeof x === "object" &&
    x !== null &&
    "code" in x &&
    typeof x.code === "string"
  ) {
    return x.code;
  }
  return undefined;
}

describe("environment pin — undici blocked-redirect rejection shape", () => {
  it("rejects with the two-half shape the recognizer depends on", async () => {
    await withRedirectPair(async (redirectorURL, targetHits) => {
      const err = await rejectionOf(() => attempt(redirectorURL, "error"));
      const cause = causeOf(err);

      expect(messageOf(cause)).toBe(REDIRECT_BLOCKED_MESSAGE);
      expect(codeOf(cause)).not.toBeDefined();
      expect(err).toBeInstanceOf(Error);
      expect(targetHits()).toEqual(0);
    });
  });

  it("CONTROL: the same fixture with redirect:'follow' resolves and hits the target", async () => {
    await withRedirectPair(async (redirectorURL, targetHits) => {
      const response = await attempt(redirectorURL, "follow");

      expect(response.status).toBe(204);
      expect(targetHits()).toBe(1);
    });
  });
});
