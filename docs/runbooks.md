# Operational Runbooks

One entry per `operational_alert` name. Each alert fires **once per process**
(first occurrence after a deploy/restart); later occurrences of the same
condition stay visible through per-request events — each entry's **Impact**
describes the exact steady state, because for alerts that open the breaker
most later lines carry the breaker's fast-fail shape, not this alert's
class. The responder arrives holding the alert's log line, so every entry
starts from it.

Entry format: **Alert → Impact → Meaning → Diagnosis → Fix → Verify**.
Diagnosis steps are ordered most-likely-first, with copy-pasteable commands
and the expected output of each.

Alerts covered:

- [`upstream_auth_failure`](#upstream_auth_failure)
- [`upstream_quota_exhausted`](#upstream_quota_exhausted)
- [`upstream_redirect_blocked`](#upstream_redirect_blocked)

---

## upstream_auth_failure

**Alert line**

```json
{"level":50,"event":"operational_alert","alert":"upstream_auth_failure","req_id":"..."}
```

**Impact**

Total unavailability until the deployment credential is fixed, in two
phases. Attempts that reach the upstream fail with `502
{"code":"upstream_auth_failure"}`, each recording a circuit breaker
FAILURE; once the breaker opens, requests fast-fail with `503
{"code":"circuit_breaker_open"}` (`error_class:
"upstream-retry-exhausted"`) without touching the upstream, and only the
periodic HALF_OPEN probe re-records the 502. The 502 code going quiet in
the logs is the breaker working, not the condition ending. It never
self-heals — only operator action ends it.

**Meaning**

The upstream rejected the **deployment's** credential (`OPENAI_API_KEY`)
with a 401. This is not the tenant-facing 401: a tenant with a bad `lkey_`
key gets 401 at the gateway's front door and can fix their own key. Here the
failing credential is the operator's, so tenants see a 5xx — a passthrough
401 would send every tenant rotating keys that were never the problem.
Counting FAILURE toward the shared breaker is safe for this class because no
tenant input can reach the upstream auth headers (they are deployment boot
config, pinned by test).

**Diagnosis**

The `error_class` on `req_complete` lines only proves the upstream *said*
401 — confirm the credential independently, from inside the deployment
(`fly ssh console`) so you test the exact secret the process uses:

```sh
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "Authorization: Bearer $OPENAI_API_KEY" "$OPENAI_BASE_URL/models"
```

- `401` → confirmed. Discriminate on the provider dashboard:
  - the key was revoked or rotated on the provider side → **fix A**
  - the deployment carries the wrong secret (dev key in prod, typo, never
    set) → **fix B**
- `200` → the credential works from here; the 401 may have been transient
  or already fixed. Watch `req_complete` lines before acting.

**Fix**

- **A — key revoked/rotated at the provider:** generate a new key in the
  provider dashboard, then `fly secrets set OPENAI_API_KEY=…`
- **B — wrong secret in the deployment:** set the correct value the same
  way.

`fly secrets set` triggers a redeploy: the new process starts with a fresh
(CLOSED) breaker and a re-armed alert.

**Verify**

Re-run the diagnosis curl — expect `200`. Then one real request through the
gateway must answer `200`, and the post-redeploy log must stay quiet: a new
`upstream_auth_failure` alert after the redeploy means still broken. Do not
outsource verification to the breaker's HALF_OPEN probe — that gambles a
real tenant request on your fix.

---

## upstream_quota_exhausted

**Alert line**

```json
{"level":50,"event":"operational_alert","alert":"upstream_quota_exhausted","req_id":"..."}
```

**Impact**

Every request fails with `502 {"code":"upstream_quota_exhausted"}` until
the upstream account has quota again. Unlike the other two alerts, each
attempt records INCONCLUSIVE, not FAILURE: the breaker stays CLOSED and
every request still round-trips to the upstream and comes back a 429. Retry
is ineligible. The condition does not self-heal before the provider's
billing reset.

**Meaning**

The upstream answered `429` with `insufficient_quota`: the deployment's own
account ran out of budget. The account belongs to the operator, so tenants
see a 5xx — they cannot fix the operator's billing. The breaker deliberately
does **not** count this as FAILURE, for two reasons. First, a quota 429 is
not proof the next request fails: billing states move on their own (top-ups
land asynchronously, holds release, limits apply per project or window), so
unlike a revoked credential the condition is not deterministic per request —
INCONCLUSIVE is the honest verdict. Second, quota exhaustion is the one
condition a tenant can *induce* (many expensive but successful requests);
counting it as FAILURE would let a single tenant open the shared breaker and
cut availability for everyone. Note the neighbors this entry does **not**
cover: an upstream `429 rate_limit_exceeded` (RPM/TPM burst) passes through
verbatim with its Retry-After, and a context-window overflow is a 400 —
both are request-scoped, neither summons the operator.

**Diagnosis**

Confirm on the provider's usage/billing dashboard (credits exhausted or
hard limit reached). Out-of-band reproduction from inside the deployment:

```sh
curl -s -X POST "$OPENAI_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $OPENAI_API_KEY" -H "Content-Type: application/json" \
  -d '{"model":"<model>","messages":[{"role":"user","content":"ping"}],"max_tokens":1}'
```

- Response body containing `"code": "insufficient_quota"` → confirmed
  (`code` is the field the gateway itself reads to classify; `type` is not).
- While there, check **who drove the spend**: count `req_start` lines per
  `tenant_id` in the window before the alert (`req_complete` carries no
  tenant field — join through `req_id` if you need statuses). A single
  tenant dominating usage is the cost-exhaustion pattern and deserves a
  follow-up beyond the refill.

**Fix**

Top up credits or raise the limit on the provider dashboard. No deployment
change is involved.

**Verify**

Re-run the diagnosis curl — expect a normal completion instead of 429, then
one real request through the gateway answering `200`. Caveat specific to
this entry: fixing billing does **not** restart the process, so the
once-per-process alert flag stays consumed — silence proves nothing here.
Verify through `req_complete` lines returning to `error_class: null`, never
through the absence of a new alert.

---

## upstream_redirect_blocked

**Alert line**

```json
{"level":50,"event":"operational_alert","alert":"upstream_redirect_blocked","req_id":"..."}
```

**Impact**

Total unavailability until the endpoint configuration changes, in two
phases: attempts that reach the upstream fail with `502
{"code":"upstream_redirect_blocked"}` and record circuit breaker FAILUREs;
once the breaker opens, requests fast-fail with `503
{"code":"circuit_breaker_open"}` (`error_class:
"upstream-retry-exhausted"`), and only the periodic HALF_OPEN probe
re-records the 502. This condition never self-heals.

**Meaning**

The configured upstream endpoint (`OPENAI_BASE_URL`) answered a 3xx
redirect. The gateway refuses to follow redirects (fail-closed egress):
following one would replay the request — tenant prompt body and the
deployment credential included — against a host nobody vetted. A redirect
has exactly three possible causes, all operator-side, discriminated below.

**Diagnosis**

Step 1 — reproduce and read the redirect target:

```sh
curl -s -o /dev/null -D - -X POST "$OPENAI_BASE_URL/chat/completions" | sed -n '1p;/^[Ll]ocation/p'
```

- `HTTP/… 3xx` with a `Location:` header → confirmed from here; note the
  Location value and go to Step 2.
- No 3xx (401/404/200…) → the redirect does not reproduce from your
  machine: suspect **cause 3** (an intermediary on the deployment's network
  path, not on yours). Re-run the same curl from inside the deployment
  (`fly ssh console`) before concluding anything.

Step 2 — classify by comparing `Location` against `OPENAI_BASE_URL`:

| Location looks like | Cause |
|---|---|
| Same host, different scheme/path (`http`→`https`, missing `/v1`, trailing slash) | **1 — stale endpoint config** (most common) |
| Different host, and the provider announced a migration (status page, changelog) | **2 — upstream legitimately moved** |
| Different host, no announcement — unknown domain, corporate proxy, captive portal | **3 — interception on the path** (security-relevant) |

**Fix**

- **Cause 1 — stale config:** set `OPENAI_BASE_URL` to the final URL
  (scheme and path exactly as the provider documents it) and redeploy:
  `fly secrets set OPENAI_BASE_URL=https://…`
- **Cause 2 — upstream moved:** vet the new host first (provider docs, TLS
  certificate), then update `OPENAI_BASE_URL` to the *documented* new URL.
  Never copy the `Location` value blindly — that would outsource the egress
  decision to whoever produced the redirect.
- **Cause 3 — interception:** do **not** point the config at the Location
  target. Investigate the network path (DNS answers, proxy env vars, egress
  rules) from the deployment environment. The gateway held fail-closed: no
  tenant data left the boundary. Escalate before changing any config.

**Verify**

Re-run the Step 1 curl: the answer must no longer be 3xx (a 401 without
credentials is success here — it proves you reached the real upstream, which
is refusing auth as expected). Then send one real request through the
gateway and confirm `200` plus a quiet log: after the restart the alert is
re-armed, so a correctly configured deployment emits no new
`operational_alert`.
