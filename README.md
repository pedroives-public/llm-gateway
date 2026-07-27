# llm-gateway

Self-hosted LLM API gateway. V1 ships an authenticated buffered proxy with a
full reliability envelope (retry, circuit breaker, timeouts, response cap,
cost caps). Rate limiting, idempotency replay, semantic caching, and SSE
streaming are roadmap (V1.1+).

Operating model: one deployment per operating company; tenants are the
company's internal consumers (teams/apps/keys), and the upstream credential
belongs to the deployment operator — never to a tenant.

## Stack

- **Runtime:** Node.js 22 LTS + TypeScript
- **HTTP:** Fastify
- **Data:** PostgreSQL (Drizzle ORM), Redis (rate limiting, idempotency, cache)
- **Auth:** HMAC-SHA-256 with pepper for API key validation
- **Observability:** Pino (structured logs), OpenTelemetry, Prometheus
- **Testing:** Vitest + Testcontainers
- **Deploy:** Docker + Fly.io
- **CI:** GitHub Actions

## Proxy route (V1 — buffered)

`POST /v1/chat/completions` — OpenAI-compatible chat completions, buffered
only: `stream: true` is rejected with `400` until streaming lands in V1.1.
Auth via `Authorization: Bearer lkey_...`. The body must carry `model` and a
non-empty `messages[]`. Cost-containment caps on the shared operator account:
`max_tokens`/`max_completion_tokens` ≤ 16384 (injected when absent) and
`n` fixed at 1.

Reliability envelope per request: at most one retry, permitted only on proven
non-execution (pre-send network failure on a pinned whitelist, or an explicit
upstream `429`/`5xx` rejection); `Retry-After` honored only from `429`/`503`
(delta-seconds); 30-second wall-clock; 1 MiB response cap; circuit breaker
(5 failures / 30 s → open 60 s, single half-open probe).

### Error classes

Every error response carries an `x-gateway-error-class` header:

| Class | Meaning |
|---|---|
| `client-fault` | The consumer's request is at fault; upstream 4xx/429 pass through verbatim |
| `gateway-fault` | The gateway terminated the request (wall-clock, response cap, internal error) |
| `upstream-fault` | Upstream responded unusably (e.g. `200` with an undecodable body) |
| `upstream-retry-exhausted` | Both attempts failed on upstream `5xx` (normalized to `502`) |
| `upstream-auth-failure` | The deployment's own upstream credential was rejected — operator action required |
| `upstream-access-denied` | Upstream denied access in the deployment's account context |
| `upstream-quota-exhausted` | The deployment's upstream account is out of quota — operator action required |

The three `upstream-*` operator-culpable classes always answer with a
deterministic sanitized `502` body — no upstream body fragment or header
reaches the consumer. Requests rejected before any upstream attempt carry
per-field `error.code` values such as `malformed_json`, `empty_body`,
`unsupported_content_type`, `stream_not_supported`, `n_not_supported`,
`max_tokens_too_large`, or `max_completion_tokens_too_large`.

### Idempotency-Key (V1: accepted, no-op)

The header is accepted and ignored: no server-side dedup, no effect on retry
(retry is gated by evidence of non-execution; neither OpenAI nor Anthropic
honor the header upstream). It gains replay semantics when the idempotency
store ships (post-V1).

### Known V1 limitations

- A large `max_tokens` can push a single generation past the 30-second
  wall-clock: the gateway answers `504`/`gateway-fault` even though the
  upstream may still bill the completed call. Prefer modest `max_tokens` in
  V1; the 120-second streaming budget arrives with V1.1.

## Fly.io Secrets

Required secrets for production deployment:

```sh
fly secrets set GATEWAY_HMAC_PEPPER="$(openssl rand -base64 32)"
fly secrets set DATABASE_URL="postgresql://..."
fly secrets set OPENAI_API_KEY="sk-..."
```

Optional:

```sh
# Override the OpenAI base URL (must use https://)
fly secrets set OPENAI_BASE_URL="https://api.openai.com/v1"
```

## Status

V1 buffered proxy feature-complete (July 2026): auth, request validation and
cost caps, retry/breaker/timeout envelope, error classification with
sanitized operator-fault responses, structured observability events.
Streaming (SSE) is the V1.1 milestone. See commit history for details.
