import { describe, it, expect, vi } from "vitest";
import net from "node:net";
import type { AddressInfo } from "node:net";
import { resolveRejection } from "../../src/upstream/rejection.js";

// Runtime pin (Node 22 / undici). The pre-send retry whitelist is
// fixture-gated: a cause.code only authorizes a retry once a real rejected
// fetch has proven its shape in this runtime. These probes hit real
// sockets/DNS — a failure after a Node upgrade or in an offline environment
// means "re-verify the shape", not necessarily a code bug.

async function rejectionOf(pending: Promise<Response>): Promise<unknown> {
  return pending.then(
    () => {
      throw new Error("fetch resolved; expected a network rejection");
    },
    (err: unknown) => err,
  );
}

function causeCodeOf(err: unknown): string | undefined {
  if (
    typeof err === "object" &&
    err !== null &&
    "cause" in err &&
    typeof err.cause === "object" &&
    err.cause !== null &&
    "code" in err.cause &&
    typeof err.cause.code === "string"
  ) {
    return err.cause.code;
  }
  return undefined;
}

// Routes the real rejection through the production recognition path and
// asserts it lands on the retry-eligible arm without touching the
// unrecognized log.
function expectPreSendProven(err: unknown): void {
  const logger = { error: vi.fn() };
  const outcome = resolveRejection(err, new AbortController().signal, logger);

  expect(outcome).toEqual({ kind: "network_failed", pre_send_proven: true });
  expect(logger.error).not.toHaveBeenCalled();
}

describe("runtime pin — pre-send network rejection shapes", () => {
  it(
    "DNS NXDOMAIN rejects as TypeError with cause.code ENOTFOUND",
    { timeout: 10_000 },
    async () => {
      // .invalid TLD is reserved (RFC 2606) — guaranteed NXDOMAIN.
      const err = await rejectionOf(
        fetch("http://gateway-fixture-probe.invalid/"),
      );

      expect(err).toBeInstanceOf(TypeError);
      expect(causeCodeOf(err)).toBe("ENOTFOUND");
      expectPreSendProven(err);
    },
  );

  it(
    "closed port rejects as TypeError with cause.code ECONNREFUSED",
    { timeout: 10_000 },
    async () => {
      // Bind to learn a free port, close it, then connect to the dead port.
      const srv = net.createServer();
      await new Promise<void>((resolve) => srv.listen(0, "127.0.0.1", resolve));
      const { port } = srv.address() as AddressInfo;
      await new Promise<void>((resolve, reject) => {
        srv.close((closeErr) => (closeErr ? reject(closeErr) : resolve()));
      });

      const err = await rejectionOf(fetch(`http://127.0.0.1:${port}/`));

      expect(err).toBeInstanceOf(TypeError);
      expect(causeCodeOf(err)).toBe("ECONNREFUSED");
      expectPreSendProven(err);
    },
  );

  it(
    "TCP connect blackhole rejects with cause.code UND_ERR_CONNECT_TIMEOUT",
    { timeout: 15_000 },
    async () => {
      // 192.0.2.0/24 is TEST-NET-1 (RFC 5737) — packets are dropped, the
      // handshake never completes. Takes the full undici default connect
      // timeout (~10 s); shortening it would require the undici package.
      const err = await rejectionOf(fetch("http://192.0.2.1:81/"));

      expect(err).toBeInstanceOf(TypeError);
      expect(causeCodeOf(err)).toBe("UND_ERR_CONNECT_TIMEOUT");
      expectPreSendProven(err);
    },
  );
});
