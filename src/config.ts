const KNOWN_DEFAULT_PEPPERS = new Set(["change-me-min-32-chars-recommended"]);

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
