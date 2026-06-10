import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http from "node:http";
import type { AddressInfo } from "node:net";

// Runtime pin (Node 22 / undici). When an upstream fetch rejects, the gateway
// decides "this rejection came from OUR abort" by object identity:
// err === signal.reason. The WHATWG spec would allow the runtime to
// structured-clone the abort reason, but this runtime rejects with the very
// object passed to abort(). If this file fails after a Node upgrade, that
// identity assumption is broken — re-verify it before trusting the
// abort-recognition logic.
describe("runtime pin — fetch abort rejection identity", () => {
  let server: http.Server;
  let url: string;

  beforeAll(async () => {
    server = http.createServer(() => {
      // never responds — keeps the fetch in flight until the abort fires
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const { port } = server.address() as AddressInfo;
    url = `http://127.0.0.1:${port}/`;
  });

  afterAll(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
  });

  async function rejectionOf(pending: Promise<Response>): Promise<unknown> {
    return pending.then(
      () => {
        throw new Error("fetch resolved; expected an abort rejection");
      },
      (err: unknown) => err,
    );
  }

  it("abort(customObject) mid-flight rejects with the SAME object by identity", async () => {
    const controller = new AbortController();
    const reason = { kind: "wall_clock_expired" };
    const pending = fetch(url, { signal: controller.signal });
    setTimeout(() => controller.abort(reason), 50);

    const err = await rejectionOf(pending);

    expect(err).toBe(reason);
    expect(err).toBe(controller.signal.reason);
  });

  it("bare abort() ALSO passes the identity check — identity alone cannot prove the reason is ours", async () => {
    const controller = new AbortController();
    const pending = fetch(url, { signal: controller.signal });
    setTimeout(() => controller.abort(), 50);

    const err = await rejectionOf(pending);

    expect(err).toBe(controller.signal.reason);
    expect(err).toBeInstanceOf(DOMException);
    const name = err instanceof DOMException ? err.name : "<not a DOMException>";
    expect(name).toBe("AbortError");
  });
});
