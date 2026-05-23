const KNOWN_DEFAULT_PEPPERS = new Set(["change-me-min-32-chars-recommended"]);

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
