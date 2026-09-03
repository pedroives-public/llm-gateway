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

## Load probe: gateway overhead and the admission wedge (measured 2026-08-29 and 2026-09-02)

Method, reproducible by anyone with a Fly account: a staging copy of the
gateway (`llm-gateway-probe`: the production image, same VM size
`shared-cpu-1x` / 256 MB, same region and edge concurrency limits, its own
Postgres and pepper, no auto-stop) pointed at a mock upstream
(`llm-gateway-mock`, a fixed chat completion whose delay is chosen per
request by the model name) and a k6 generator on the same private network
and region. Closed model (constant VUs, no think time); grid = mock delay
{0, 500} ms x gateway VUs {1, 10, 41, 50}, plus the mock reached directly
from the same machine at {1, 10} VUs as the baseline; 10 s warm-up
discarded, 45 s measured, 3 rounds per cell, median across rounds with the
spread. Overhead = gateway path minus direct path at the same delay (both
paths reach the mock over its public hostname, so edge and TLS cost
cancels). 503 `admission_full` refusals are counted apart, never mixed
into latency. Staging Postgres is Fly Postgres in gru; production is Neon
in sa-east-1, so the tracking write's share of the overhead is not
identical to production's. Tooling is local operator scripts, not part of
the repository.

### 2026-08-29, release v27: the gate wedges

Round 1 in grid order; each cell is 45 s of measurement.

| cell | ok | 503 admission_full | p50 (ok only) |
|---|---|---|---|
| gateway, delay 0, 1 VU | 3 153 | 0 | 13.0 ms |
| gateway, delay 0, 10 VUs | 15 875 | 0 | 23.8 ms |
| gateway, delay 0, 41 VUs | 2 521 | 14 019 | 78.7 ms |
| gateway, delay 0, 50 VUs | 0 | 11 977 | - |
| gateway, delay 500, 1 VU | 0 | 3 516 | - |
| gateway, delay 500, 10 VUs | 0 | 4 228 | - |
| gateway, delay 0, 1 VU (round 2) | 0 | 3 914 | - |

From the 41-VU cell on, every request was refused for the rest of the run,
including a single VU against a 500 ms upstream; `/health` answered 200
throughout and nothing reached the logs. Restarting the process cured it.
Cause: the post-auth admission gate returned its slot only through
Fastify's `onResponse` and `onRequestAbort` hooks, and neither fires when
a client drops the socket after the body was read and before the
response. The Fly edge, at its concurrency hard limit during the 41/50-VU
cells, closed in-flight connections and leaked one slot each; 41 leaks
wedged the gate for every tenant until restart. Fixed in #41 (release on
the socket-level `close` plus a synchronous check at acquisition;
regression pinned on a real socket in
`tests/reliability/admission-slot-release.test.ts`), and the post-deploy
proof of boot gained an authenticated request that crosses the gate (#42).

### 2026-09-02, release v28: the same grid on the fixed release

The recovery cell (delay 500, 1 VU, right after the 41/50-VU cells) came
back with 89 ok and 0 refusals in all three rounds; no refusal appeared in
any 1- or 10-VU cell of the run.

Mock delay 500 ms (the regime an LLM upstream lives in):

| VUs | ok/round | 503 admission_full/round | gateway p50 (spread) | overhead p50 | overhead p95 (indicative) |
|---|---|---|---|---|---|
| 1 | 89 | 0 | 509.3 ms (509.2..510.2) | 6.4 ms | 9.7 ms |
| 10 | 880 | 0 | 511.2 ms (510.7..511.8) | 8.4 ms | 24.9 ms |
| 41 | 3 605 | 71 (48..108) | 510.8 ms (510.6..511.0) | 8.0 ms | 29.2 ms |
| 50 | 782 | 4 646 | 1 758.6 ms (1 685..1 840) | not an overhead figure, see below | |

Mock delay 0 ms (a 1 ms upstream; a stress regime, not a serving one):

| VUs | ok/round (spread) | 503 admission_full/round | gateway p50 (spread) | overhead p50 |
|---|---|---|---|---|
| 1 | 1 784 (1 744..6 364) | 0 | 7.0 ms (6.6..7.3) | 5.9 ms |
| 10 | 1 225 (1 172..24 163) | 0 | 322.9 ms (15.8..323.0) | 321.4 ms (round 1: 14.3 ms) |
| 41 | 782 | 5 119 | 1 278.6 ms | saturated |
| 50 | 667 | 5 880 | 1 516.8 ms | saturated |

Readings:

- With a 500 ms upstream the gateway adds 6 to 8 ms at p50 from 1 to 41
  concurrent requests, with a spread under 1 ms at 41. The gateway's own
  handler clock (38 095 `req_complete` events harvested from the logs, a
  lossy capture) puts the in-handler overhead at 0 ms p50 and 1 ms p99:
  the 6 to 8 ms live outside the handler, in the auth lookup, the tracking
  write and the edge hop.
- The 41-VU ceiling refuses about 2% at exactly 41 VUs (a slot is
  returned one tick after the response leaves, and a VU with no think time
  can arrive inside that tick); it oscillates, it does not accumulate.
- The 50-VU cells carried one or two non-HTTP failures per round (a
  connection closed before any response, once an empty 502): both come
  from the edge above its hard limit of 48, not from the gateway, whose
  error responses always carry a JSON envelope.
- A single shared CPU saturates above 41 VUs with a 1 ms upstream, and the
  10-VU cell ran 20 times slower in rounds 2 and 3 than in round 1 with no
  refusal at all. Attributed on the Fly instance dashboard: the first
  heavy cell reached about 95% CPU; every heavy cell after it shows the
  `throttled` band with utilization pinned near 20%. The `shared-cpu-1x`
  burst quota is spent by the first 41/50-VU cells and the rest of the run
  is served throttled. Memory stayed between 84 and 135 MiB of 256
  throughout; the instance concurrency panel plateaus at 41 (the gateway's
  ceiling) and at 48 (the edge's hard limit) in the 41- and 50-VU cells.
- At 50 VUs with a 500 ms upstream the admitted requests rise to 1.7 s
  p50, from two causes. Refusal is post-auth, so each `503
  admission_full` still costs an HMAC and a database lookup, and at
  roughly 100 refusals per second that path alone loads the instance; and
  the edge queues the requests above its hard limit of 48 instead of
  refusing them. Input for the rate-limiting phase: the refusal has to
  become cheaper than the lookup, or bounded per principal.

Cold start (auto-stop machine woken by a request, 10 samples, 2026-08-29):
median 7.65 s, range 7.29 to 8.39 s.

## Status

V1 buffered proxy feature-complete (July 2026): auth, request validation and
cost caps, retry/breaker/timeout envelope, error classification with
sanitized operator-fault responses, structured observability events.
Per-phase admission control and the CD pipeline landed in August 2026.
Streaming (SSE) is the V1.1 milestone. See commit history for details.
