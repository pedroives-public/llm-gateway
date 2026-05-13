# llm-gateway

Multi-tenant LLM API gateway with rate limiting, retry, idempotency, and semantic caching.

## Stack

- **Runtime:** Node.js 22 LTS + TypeScript
- **HTTP:** Fastify
- **Data:** PostgreSQL (Drizzle ORM), Redis (rate limiting, idempotency, cache)
- **Auth:** HMAC-SHA-256 with pepper for API key validation
- **Observability:** Pino (structured logs), OpenTelemetry, Prometheus
- **Testing:** Vitest + Testcontainers
- **Deploy:** Docker + Fly.io
- **CI:** GitHub Actions

## Status

Phase 1 in progress (May 2026). See commit history for current state.
