const KNOWN_DEFAULT_PEPPERS = new Set(["change-me-min-32-chars-recommended"]);

// Inbound time budget. Request cap: 20s = the 256 KiB body limit at a 256 kbps
// committed-bandwidth floor (8s) with 2x margin; headers (~1 KiB): 10s. Node
// enforces both via a periodic sweep — a 5s interval bounds the worst-case kill
// at 25s, under the 30s handler wall-clock, so an upload never outlives its
// slot. requestTimeout is a top-level Fastify option; the rest createServer-only.
export const REQUEST_TIMEOUT_MS = 20_000;
export const HTTP_SERVER_OPTIONS = {
  headersTimeout: 10_000,
  connectionsCheckingInterval: 5_000,
} as const;

// Post-auth admission capacity: floor(117 MiB operational MemAvailable on the
// deploy target x 0.85 safety factor / 2.4 MiB per in-flight buffered request).
// The Fly edge hard_limit (48) is the blind backstop sitting above this gate.
export const ADMISSION_CAPACITY_POST_AUTH = 41;

// Pre-auth admission capacity: min of socket-buffer headroom (floor(19 MiB
// spare / 1.38 MiB tcp_rmem max) = 13) and the auth-phase drain chain
// (10 DB connections / 2 s statement_timeout x 2 s residence = 10, binds).
// Inputs pinned by tests/reliability/admission-derivation-pin.test.ts.
export const ADMISSION_CAPACITY_PRE_AUTH = 10;

// Streaming total-duration deadline, derived as a corridor and not as a point:
// - above 165 s, the slowest legitimate completion measured (gpt-5, reasoning
//   effort medium, chat completions, 2026-08-26): headers at 133 s, after a
//   silent reasoning phase, then 32 s of generation;
// - at least 1.5 x 165 s = 247.5 s, the margin over that measurement;
// - strictly under 300 s, the default outbound dispatcher's headersTimeout
//   (its bodyTimeout is also 300 s), so the gateway's own deadline fires first.
// 280 s is the point chosen inside the corridor. Output that still fits after
// a 133 s silent phase: (280 - 133) s x ~62 tokens/s (4,076 tokens in 66 s,
// same day) = ~9k tokens. Cost: 41 post-auth slots each held up to 280 s admit
// at most ~8.8 streaming requests per minute when every slot runs to the end.
// Corridor pinned by tests/reliability/total-duration-derivation-pin.test.ts.
export const STREAM_TOTAL_DURATION_MS = 280_000;

// Bounds the breaker's whole recovery probe, up to `[DONE]`. Measured from the
// production machine (new connection, gpt-4o-mini, max_tokens 1, N=20 each):
// 2026-10-31, off-peak, maximum 2157 ms; 2026-11-04, US business hours, median
// 917 ms and maximum 4576 ms, the wait falling before the response head. 10 s
// keeps more than twice the worst of the 40 attempts. The error is asymmetric:
// too short costs 60 s more of OPEN with a healthy upstream; too long costs
// tenants nothing, since HALF_OPEN fast-fails them like OPEN, only a few
// seconds more before the next attempt when the upstream is dead.
export const PROBE_CLOCK_MS = 10_000;

// The breaker's own bound on a recovery probe attempt that has not voted.
// Twice PROBE_CLOCK_MS: the probe's clock must fire first (its cleanup was
// measured at about 30 ms on 2026-11-06), so this fires only when the probe
// code itself hangs, a gateway defect. A long bound costs tenants nothing,
// since HALF_OPEN fast-fails them like OPEN; it only delays the next attempt
// in that defect case.
export const PROBE_BACKSTOP_MS = 2 * PROBE_CLOCK_MS;

export function getOpenAIApiKey(): string {
  const key = process.env["OPENAI_API_KEY"];
  if (!key) {
    throw new Error("OPENAI_API_KEY environment variable is required");
  }
  return key;
}

export function getOpenAIBaseUrl(): string {
  const override = process.env["OPENAI_BASE_URL"];
  if (!override) {
    return "https://api.openai.com/v1";
  }
  if (!override.startsWith("https://") && process.env["NODE_ENV"] !== "test") {
    throw new Error(
      `OPENAI_BASE_URL must use https:// outside NODE_ENV=test, got: ${override}`,
    );
  }
  return override;
}

export function getProbeModel(): string {
  const value = process.env["BREAKER_PROBE_MODEL"];
  if (value === undefined || value === "" || value !== value.trim()) {
    // The received value is left out on purpose: a secret pasted into the
    // wrong variable must not reach the boot log.
    throw new Error("BREAKER_PROBE_MODEL must name a model");
  }
  return value;
}

export function getPepper(): string {
  const pepper = process.env["GATEWAY_HMAC_PEPPER"];
  if (!pepper) {
    throw new Error("GATEWAY_HMAC_PEPPER environment variable is required");
  }
  if (pepper.length < 32) {
    throw new Error("GATEWAY_HMAC_PEPPER must be at least 32 characters");
  }
  if (KNOWN_DEFAULT_PEPPERS.has(pepper)) {
    throw new Error(
      "GATEWAY_HMAC_PEPPER must be changed from the default value. Generate one with: openssl rand -base64 32",
    );
  }

  return pepper;
}

export function getStreamingEnabled(): boolean {
  const value = process.env["STREAMING_ENABLED"];
  if (value === undefined) {
    return false;
  } else if (value === "true") {
    return true;
  } else if (value === "false") {
    return false;
  } else {
    // The received value is left out on purpose: a secret pasted into the
    // wrong variable must not reach the boot log.
    throw new Error('STREAMING_ENABLED must be either "true" or "false"');
  }
}
