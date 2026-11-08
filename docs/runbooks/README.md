# Operational Runbooks

One entry per `operational_alert` name. Most alerts fire **once per process**
(first occurrence after a deploy/restart) — `auth_db_unavailable` fires once
per **episode** instead (its entry explains); later occurrences of the same
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
- [`upstream_probe_rejected`](#upstream_probe_rejected)
- [`auth_db_cause_unknown`](#auth_db_cause_unknown)
- [`auth_db_unavailable`](#auth_db_unavailable)

Scheduled operations (not alert-driven) have their own files:

- [Rotating the Fly deploy token](fly-deploy-token-rotation.md) — monthly, two days before expiry.

Request endings that raise no alert have their own files too:

- [`TOTAL_TIMEOUT`: a streaming request outlived its deadline](streaming-total-timeout.md) — reachable only where the streaming flag is on.

Recovery-probe lines that raise no alert:

- [`breaker_probe_clock_expired`: the recovery probe outlived its clock](breaker-probe-clock-expired.md) — only while the breaker is HALF_OPEN.
- [`breaker_probe_backstop`: a recovery probe never finished](breaker-probe-backstop.md) — error level; only a gateway defect produces it.
- [Unrecognized upstream rejection with AbortError: the gateway cancelled its recovery probe](breaker-probe-cancel-abort.md) — error level; a known false error that follows a backstop line.

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
"upstream-retry-exhausted"`) without touching the upstream. The breaker's
own recovery probe, a synthetic request sent every 60 seconds with no tenant
involved, keeps meeting the same rejection (`breaker_probe_result` with
outcome `FAILURE`), so tenant `502` lines stop. The 502 code going quiet in
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

Re-run the diagnosis curl — expect `200`. Then wait for the breaker's next
recovery probe, at most about 70 seconds: a `breaker_probe_result` with
outcome `SUCCESS` followed by `cb_state_change` to `CLOSED` shows the gateway
reached the upstream with the fixed credential. The probe is a synthetic
request, so no tenant request is spent on the check. Then one real request
through the gateway must answer `200`, and the post-redeploy log must stay
quiet: a new `upstream_auth_failure` alert after the redeploy means still
broken.

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

The recovery probe has a separate rule: if the deployment-owned probe receives
`insufficient_quota`, it records FAILURE and the breaker stays OPEN until the
next probe in 60 seconds. The once-per-process alert has already fired; the
probe's `breaker_probe_result` event continues to show each later outcome. This
does not change the INCONCLUSIVE rule for tenant requests, which can induce
quota exhaustion and must not open the shared breaker.

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
"upstream-retry-exhausted"`). The breaker's own recovery probe, a synthetic
request every 60 seconds, meets the same redirect and keeps the breaker
OPEN (`breaker_probe_result` with outcome `FAILURE`), so tenant `502` lines
stop. This condition never self-heals.

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

---

## upstream_probe_rejected

**Alert line**

```json
{"level":50,"event":"operational_alert","alert":"upstream_probe_rejected","req_id":"breaker_probe"}
```

No tenant request carries this alert, so `req_id` is the fixed value
`breaker_probe` and joins to nothing. The adjacent `breaker_probe_result` event
records the attempt and outcome, but deliberately omits the upstream status and
body. Use the diagnosis request below to inspect the exact response; upstream
content never reaches the logs.

**Impact**

It depends on the status the upstream answered, in two opposite ways:

- `400`, `404`, `413` or `422`: the breaker **closes**. Tenants are served again,
  but this probe proved nothing about recovery, because the upstream only refused
  the probe's own request. If the upstream is still sick, the breaker reopens on
  tenant failures and the cycle repeats.
- `403`: the breaker **stays OPEN**. Every tenant receives `503
  {"code":"circuit_breaker_open"}` and a new probe runs every 60 seconds. It
  ends only when the account regains access or the setting changes.

**Meaning**

The gateway recovers the breaker with a small synthetic request of its own, and
the upstream refused that request. No tenant input reaches it: the model comes
from the deployment setting `BREAKER_PROBE_MODEL`, and the rest of the request is
fixed. So the fault is in the deployment's configuration or in the account, never
in a tenant. The usual causes are a model name that does not exist, a model that
rejects `stream` or `max_tokens`, and an account without access to that model.

The four statuses close the breaker on purpose: holding every tenant in `503`
because of a wrong setting would be an outage with a healthy upstream. A `403` does
not, because a denied account can be denying tenant requests too.

**Diagnosis**

Send the probe's exact request from inside the deployment (`fly ssh console`), so
it uses the same secret and the same setting:

```sh
curl -sS -w "\n%{http_code}\n" -X POST "$OPENAI_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $OPENAI_API_KEY" -H "Content-Type: application/json" \
  -d "{\"model\":\"$BREAKER_PROBE_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"stream\":true,\"max_tokens\":1}"
```

The gateway never logs the upstream's body, so this is where you read the reason:

- `404`, or a body that says the model does not exist: **fix A**.
- `400` or `422` naming a parameter: the model rejects the probe's shape, **fix A**.
- `403`: the account has no access. Compare with the same request using a model
  the tenants use. If that one answers `200`, **fix A**. If it also answers `403`,
  **fix B**.
- `413`: the upstream or something on the path refuses even this small body.
  Re-run with another model and with a direct request from your machine, and
  suspect the path before the setting.
- `200` ending in `data: [DONE]`: the condition is gone from here. Compare the
  setting in `fly.toml` with the running value before closing the incident.

**Fix**

- **A, wrong or unsuitable model:** change `BREAKER_PROBE_MODEL` in the `[env]` of
  `fly.toml` to a small model that accepts the request above, and deploy.
- **B, account without access:** restore the access on the provider's dashboard.
  Nothing in the deployment changes. If tenants are failing too, this is the same
  incident as their `403`s.

**Verify**

Re-run the diagnosis request: it must answer `200` and end in `data: [DONE]`. That
is the proof, because the probe runs only while the breaker is OPEN: with the
breaker CLOSED, a quiet log says nothing about the setting. The boot check only
confirms the setting is present and well formed, not that the upstream accepts it.
After fix A the deploy restarts the process and re-arms the alert. After fix B the
alert stays consumed, so during an open breaker read the probe's next event, which
must report `SUCCESS` within 60 seconds.

---

## auth_db_cause_unknown

**Alert line**

```json
{"level":50,"event":"operational_alert","alert":"auth_db_cause_unknown","req_id":"..."}
```

**Impact**

None beyond the failure already underway: requests on this branch were
already answering `503 {"error":"Service Unavailable"}` (auth DB failure).
The alert marks a **visibility** gap, not a new outage: at least one auth DB
failure occurred whose cause the log vocabulary could not identify, so the
operator cannot tell from the logs WHY auth is failing. Later occurrences of
the same form stay visible as `Auth DB lookup failed` lines carrying
`UNKNOWN` fields.

**Meaning**

The auth DB catch logs only the `{cause_code, cause_name}` pair extracted
from the thrown error (an allowlist — driver detail like addresses, protocol
fields and stacks is deliberately dropped). This alert means the extraction
came back with `UNKNOWN` in at least one field: the error's shape is outside
the vocabulary. The signal is the classification miss itself — there is no
ErrorClass on this branch, so the catch summons the operator directly. Once
per process; the intended lifecycle is the natural cycle: extend the
vocabulary → deploy → fresh process → any remaining unknown form re-alerts.

**Diagnosis**

Start from the adjacent failure line — same `reqId`, msg
`Auth DB lookup failed` — and read what did survive extraction:

- one field known, the other `UNKNOWN`: the partial identity narrows the
  family (an SQLSTATE such as `57014` without a name still says
  "server-side statement timeout").
- both `UNKNOWN`: the throw did not carry `code`/`name` strings at all —
  suspect a non-driver throw on the query path.

Correlate the timestamp with what changed around it: a driver or ORM bump
in a recent deploy (new error shape), a database provider incident window
(status page), or a new code path touching the auth query. The log
deliberately carries no further driver detail, so to see the full shape
reproduce the suspected condition locally (testcontainer or probe script)
rather than fishing in production.

**Fix**

Extend the vocabulary: teach `authDbCause` (`src/middleware/auth.ts`) the
observed shape, transcribing it field-for-field into a pinned test the way
`tests/middleware/auth-log-leak.test.ts` pins the connection and server
shapes. Deploy.

**Verify**

After the deploy, the same failure condition must produce an
`Auth DB lookup failed` line with both fields identified. A new
`auth_db_cause_unknown` alert after the vocabulary extension is the natural
cycle working, not a regression: another unknown form remains — repeat the
loop for it.

---

## auth_db_unavailable

**Alert line**

```json
{"level":50,"event":"operational_alert","alert":"auth_db_unavailable","req_id":"..."}
```

**Impact**

An episode of auth DB unavailability is underway: failures crossed the
episode threshold inside its window. While the DB keeps failing, every
request on the auth path answers `503 {"error":"Service Unavailable"}` — the
gateway is down for all tenants even though the process itself is healthy
(`/health` still answers).

**Meaning**

Unlike the other alerts, this one fires once per **episode**, not once per
process. The detector opens an episode when failures cross the threshold
inside the trailing window; only a successful auth DB lookup closes it —
quiet periods close nothing. A second alert in the same process is a NEW
episode after a recovery, not a duplicate. After a restart the alert may
repeat for the same outage: at-least-once semantics, because episode state
lives in memory — the DB being monitored cannot be its own alert store.

**Diagnosis**

The per-failure lines say WHY. Read the adjacent `Auth DB lookup failed`
lines' `{cause_code, cause_name}`:

- connection-class codes (e.g. `CONNECT_TIMEOUT`): the database is
  unreachable — provider outage or endpoint/connection-string drift. Check
  the provider status page and recent config changes.
- `57014` (server-side statement timeout): the DB is reachable but not
  answering inside `statement_timeout` — suspect provider degradation or
  lock contention.
- `UNKNOWN` fields: see [`auth_db_cause_unknown`](#auth_db_cause_unknown) —
  both alerts can fire during the same episode.

**Fix**

Cause-dependent: provider incident → follow the provider's status/failover
path; endpoint or credential drift → fix the deployment config and redeploy;
sustained timeouts with a healthy provider → investigate DB load.

**Verify**

Auth traffic answers again (200/401 instead of 503) and the failure lines
stop. A later `auth_db_unavailable` line is a NEW episode — treat it as a
fresh incident, not noise from this one.
