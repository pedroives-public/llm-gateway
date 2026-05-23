import { TEST_PEPPER } from "./constants.js";
// Sets required env vars before any test worker imports application modules.
// Module-level initializers (e.g. const PEPPER = getPepper() in auth.ts) fire
// at import time — before beforeAll — so the var must be present at that point.
process.env["GATEWAY_HMAC_PEPPER"] ??= TEST_PEPPER;
