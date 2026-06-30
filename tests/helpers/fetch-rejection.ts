// Mimics the shape undici's fetch produces on a network rejection:
// TypeError("fetch failed") whose cause is an Error optionally carrying a
// string `code` (pinned empirically on Node 22 — see abort-identity-pin).
export function fetchFailed(code?: string): TypeError {
  const cause = new Error("connect failed");
  if (code !== undefined) {
    (cause as Error & { code?: string }).code = code;
  }
  return new TypeError("fetch failed", { cause });
}
