// Header names on the breaker probe's upstream request: those the
// deployment-owned adapter sets and those the runtime's fetch adds. A new name
// means something added a header to the request the upstream receives. Shared
// by the adapter-level pin and the real-app cell, so a runtime change updates
// both rulers at once.
export const PROBE_ALLOWED_HEADER_NAMES: ReadonlySet<string> = new Set([
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
