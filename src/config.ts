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
