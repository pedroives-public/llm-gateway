import { vi } from "vitest";

export type RedirectOption = RequestInit["redirect"];

export interface GlobalFetchWrapper {
  /** The redirect option of each call, as the caller wrote it. */
  readonly sentRedirects: readonly RedirectOption[];
  /** The status of each response the runtime handed back to the caller. */
  readonly responseStatuses: readonly number[];
}

// Wraps the runtime's global fetch until vi.unstubAllGlobals(): every call is
// recorded and delegated to the runtime's own fetch, so sockets and the
// runtime's abort wiring stay real. With `forceRedirect`, the delegated call
// carries that option in place of the caller's. That is the control arm of a
// pair: the same scene under another redirect option, with the adapter
// untouched, because the adapter takes no fetch parameter.
export function wrapGlobalFetch(
  forceRedirect?: RedirectOption,
): GlobalFetchWrapper {
  const runtimeFetch = globalThis.fetch;
  const sentRedirects: RedirectOption[] = [];
  const responseStatuses: number[] = [];

  vi.stubGlobal(
    "fetch",
    (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      sentRedirects.push(init?.redirect);
      const response = runtimeFetch(
        input,
        forceRedirect === undefined
          ? init
          : { ...init, redirect: forceRedirect },
      );
      // Reads the status on a side branch: the caller receives the runtime's
      // own promise, and a rejected fetch has no status to record.
      void response.then(
        (resolved) => {
          responseStatuses.push(resolved.status);
        },
        () => {},
      );
      return response;
    },
  );

  return { sentRedirects, responseStatuses };
}
