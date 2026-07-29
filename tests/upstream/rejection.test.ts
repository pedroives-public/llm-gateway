import { resolveRejection, type Logger } from "../../src/upstream/rejection.js";
import type { Outcome } from "../../src/upstream/outcome.js";
import { fetchFailed } from "../helpers/fetch-rejection.js";
import { describe, it, expect, expectTypeOf, vi } from "vitest";

// The boundary logger payload deliberately omits req_id: the caller passes a
// request-scoped child logger (Fastify's request.log) that already binds
// req_id onto every line, so the payload object itself carries only the
// rejection facts.
function makeLogger() {
  return { error: vi.fn<Logger["error"]>() };
}

function firstErrorPayload(
  logger: ReturnType<typeof makeLogger>,
): Record<string, unknown> {
  expect(logger.error).toHaveBeenCalledTimes(1);
  const call = logger.error.mock.calls[0];
  if (call === undefined) {
    throw new Error("expected logger.error to be called");
  }
  return call[0];
}

function catchFrom(fn: () => unknown): unknown {
  try {
    fn();
  } catch (thrown) {
    return thrown;
  }
  throw new Error("expected resolveRejection to throw, but it returned");
}

describe("resolveRejection", () => {
  describe("gateway-initiated aborts (rejection IS the signal reason)", () => {
    it.each(["wall_clock_expired", "response_size_cap"] as const)(
      "returns aborted{%s} when the rejection is identical to a recognized typed reason",
      (kind) => {
        const controller = new AbortController();
        controller.abort({ kind });
        const err: unknown = controller.signal.reason;
        const logger = makeLogger();

        const outcome = resolveRejection(err, controller.signal, logger);

        expect(outcome).toEqual({ kind: "aborted", abort_kind: kind });
        expect(logger.error).not.toHaveBeenCalled();
      },
    );

    it("re-throws the ORIGINAL error and logs the exact boundary payload on a bare abort()", () => {
      // A reason-less abort() passes the identity check (err === signal.reason
      // still holds for the auto-created DOMException) but carries no
      // recognizable kind — it must fall through as unrecognized.
      const controller = new AbortController();
      controller.abort();
      const err: unknown = controller.signal.reason;
      const logger = makeLogger();

      const caught = catchFrom(() =>
        resolveRejection(err, controller.signal, logger),
      );

      expect(caught).toBe(err); // same object — cause chain intact for the error handler
      const payload = firstErrorPayload(logger);
      // toEqual pins the EXACT whitelist: any extra key (raw error, stack,
      // headers, body) is a leak and must fail this cell.
      expect(payload).toEqual({
        cause_code: "UNKNOWN",
        cause_name: "UNKNOWN",
        err_name: "AbortError",
        abort_identity: true,
      });
    });

    it("re-throws and logs abort_identity:true when the typed reason carries an unrecognized kind", () => {
      const controller = new AbortController();
      controller.abort({ kind: "not_in_the_enum" });
      const err: unknown = controller.signal.reason;
      const logger = makeLogger();

      const caught = catchFrom(() =>
        resolveRejection(err, controller.signal, logger),
      );

      expect(caught).toBe(err);
      const payload = firstErrorPayload(logger);
      // The custom reason object is not an Error and carries no `name`, so the
      // top-level error name falls back to the UNKNOWN sentinel.
      expect(payload).toEqual({
        cause_code: "UNKNOWN",
        cause_name: "UNKNOWN",
        err_name: "UNKNOWN",
        abort_identity: true,
      });
    });
  });

  describe("network rejections (rejection is NOT the signal reason)", () => {
    it("returns network_failed{pre_send_proven:true} for a true-list cause code", () => {
      const err = fetchFailed("ECONNREFUSED");
      const controller = new AbortController();
      const logger = makeLogger();

      const outcome = resolveRejection(err, controller.signal, logger);

      expect(outcome).toEqual({
        kind: "network_failed",
        pre_send_proven: true,
        cause_code: "ECONNREFUSED",
        cause_name: "Error",
      });
      expect(logger.error).not.toHaveBeenCalled();
    });

    it("returns network_failed{pre_send_proven:false} for a false-list cause code", () => {
      const err = fetchFailed("ECONNRESET");
      const controller = new AbortController();
      const logger = makeLogger();

      const outcome = resolveRejection(err, controller.signal, logger);

      expect(outcome).toEqual({
        kind: "network_failed",
        pre_send_proven: false,
        cause_code: "ECONNRESET",
        cause_name: "Error",
      });
      expect(logger.error).not.toHaveBeenCalled();
    });

    it("re-throws the ORIGINAL error and logs the exact payload for an unlisted cause code", () => {
      const err = fetchFailed("EHOSTUNREACH");
      const controller = new AbortController();
      const logger = makeLogger();

      const caught = catchFrom(() =>
        resolveRejection(err, controller.signal, logger),
      );

      expect(caught).toBe(err);
      const payload = firstErrorPayload(logger);
      expect(payload).toEqual({
        cause_code: "EHOSTUNREACH",
        cause_name: "Error",
        err_name: "TypeError",
        abort_identity: false,
      });
    });

    it("re-throws and logs when the rejection's cause has no string code (short-circuit)", () => {
      // e.g. fetch("https://host:1") rejects with a cause that carries no
      // `code` at all — the open-set rejection pinned on Node 22.
      const err = fetchFailed(undefined);
      const controller = new AbortController();
      const logger = makeLogger();

      const caught = catchFrom(() =>
        resolveRejection(err, controller.signal, logger),
      );

      expect(caught).toBe(err);
      const payload = firstErrorPayload(logger);
      expect(payload).toEqual({
        cause_code: "UNKNOWN",
        cause_name: "UNKNOWN",
        err_name: "TypeError",
        abort_identity: false,
      });
    });

    it("resolves through the NETWORK path even when the signal was aborted after settlement (dirty race)", () => {
      // The fetch promise settled with a network error first; the wall-clock
      // abort landed afterwards. signal.aborted is true and signal.reason is
      // populated at catch time, but the rejection VALUE is the network error
      // — recognition must be settle-once and ignore post-settlement signal
      // state.
      const err = fetchFailed("ECONNRESET");
      const controller = new AbortController();
      controller.abort({ kind: "wall_clock_expired" });
      const logger = makeLogger();

      const outcome = resolveRejection(err, controller.signal, logger);

      expect(outcome).toEqual({
        kind: "network_failed",
        pre_send_proven: false,
        cause_code: "ECONNRESET",
        cause_name: "Error",
      });
      expect(logger.error).not.toHaveBeenCalled();
    });
  });

  it("return type admits only the rejection-path outcomes", () => {
    expectTypeOf(resolveRejection).returns.toEqualTypeOf<
      Extract<Outcome, { kind: "network_failed" | "aborted" | "redirect_blocked" }>
    >();
  });
});
