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
- **Data:** PostgreSQL (Drizzle ORM)
- **Auth:** HMAC-SHA-256 with pepper for API key validation
- **Observability:** Pino (structured logs); a Prometheus `/metrics` endpoint on
  the private network is a planned slice
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

Reliability envelope per request: at most one retry, permitted only on a
network failure proven to have happened before the request left the process
(pinned error-code whitelist). Every upstream error status is retry-ineligible
by default: no status proves the upstream did not start, and bill, the
execution. `Retry-After` on `429`/`503` is parsed (delta-seconds) but grants no
retry. 30-second wall-clock; 1 MiB response cap; circuit breaker (5 failures /
30 s → open 60 s, single half-open probe); per-phase admission control
(10 pre-auth and 41 post-auth requests in flight, immediate `503` beyond
that, no queue).

### Error classes

Every error response carries an `x-gateway-error-class` header:

| Class | Meaning |
|---|---|
| `client-fault` | The consumer's request is at fault; upstream 4xx/429 pass through verbatim |
| `gateway-fault` | The gateway terminated the request (wall-clock, response cap, internal error) |
| `upstream-fault` | Upstream responded unusably (e.g. `200` with an undecodable body) |
| `upstream-retry-exhausted` | Upstream answered `5xx` (normalized to `502`, body passed through), or the circuit breaker is open (`503`); the name predates the default-deny retry rule |
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

## Delivery metrics (measured 2026-08-28)

Computed from this repository's GitHub history (merged pull requests and the
`ci.yml` job timestamps on `main`) for the period since the deploy job
landed, 14 to 27 August 2026 (13.1 days). Anyone can recompute them with
`gh pr list` and `gh run view`.

| Metric | Value | Note |
|---|---|---|
| Deployment frequency | 16 successful production deploys on 4 distinct days | 8 of the 16 were the Dependabot batch of 14 August |
| Lead time, merge to live | median 2.3 min (min 1.5, max 10.3), n=16 | push to `main` → `flyctl deploy` → `/health` 200 → 401-envelope smoke; since 27 August a human approval gate sits in front of the deploy job (n=3, median 3.8 min) |
| Change failure rate | 0 of 16 deploys degraded production; 1 of 17 deploy jobs failed | the failed job was a false-red log-grep gate on a healthy app (removed since); one job was re-run after a Fly auto-stop × rolling-update flake |
| Time to restore | 14 min for a red `main` (eslint 10 major) to be fixed and deployed; 34 min from the false-red gate to the next green deploy | no production-degrading incident occurred in the window, so a production MTTR is undefined |

Manual deploys before the CD job (Fly releases v1 to v8, May to 13 August) are
not included.

## Status

V1 buffered proxy feature-complete (July 2026): auth, request validation and
cost caps, retry/breaker/timeout envelope, error classification with
sanitized operator-fault responses, structured observability events.
Per-phase admission control and the CD pipeline landed in August 2026.
Streaming (SSE) is the V1.1 milestone. See commit history for details.
