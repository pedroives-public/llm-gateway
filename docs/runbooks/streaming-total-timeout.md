# TOTAL_TIMEOUT: a streaming request outlived its deadline

This ending raises no `operational_alert`. The responder arrives from a `504`
reported by a consumer, or from the `req_complete` line below.

**Log line**

```json
{"level":30,"event":"req_complete","req_id":"...","status":504,"error_class":"gateway-fault","stream":true,"attempts":1,"duration_ms":280012,"upstream_duration_ms":280009,"gateway_overhead_ms":3,"retry_disposition":"ineligible","terminal":"TOTAL_TIMEOUT"}
```

**Where it can happen today**

Only where the streaming flag is on (`STREAMING_ENABLED=true`). Production
refuses to boot with the flag on until the streaming path is complete, so no
production request meets this deadline yet. Until then this entry describes
development and test deployments.

**Impact**

One request. The client gets `504` with `x-gateway-error-class: gateway-fault`
and code `total_timeout_exceeded`, and the gateway makes no further attempt on
its behalf. Whatever the upstream had already computed is lost, and it may
still be billed by the provider. The circuit breaker records INCONCLUSIVE:
other requests and other tenants are not affected, and a run of these lines
never opens the breaker.

**Meaning**

A `stream: true` request may hold an admission slot for 280 seconds, counted
once from the start of the handler and across every attempt. This line means
that deadline passed before the gateway had a response to deliver. It proves
the request outlived its budget. It does not prove the upstream failed:
silence is what a queued request, a long reasoning phase and a dead upstream
all look like from here.

If the upstream answers after the deadline, the answer does not change the
outcome: the request stays `TOTAL_TIMEOUT`, and the late answer is not logged.
Recording it is planned.

**What still fits inside the deadline**

- Measured on 2026-08-26 (gpt-5, reasoning effort `medium`, chat completions):
  the slowest completion took 165 s, with headers arriving at 133 s after a
  silent reasoning phase.
- After a 133-second silent phase, about 9k tokens of output still fit:
  (280 - 133) s x ~62 tokens/s.
- Reasoning effort `high` is not served on this route. Measured the same day,
  it passed 300 s without a response head, which is also the outbound client's
  own limit.
- With all 41 post-auth slots running to the deadline, the gateway admits at
  most about 8.8 streaming requests per minute.

**Diagnosis**

The line alone does not tell a slow request from a silent upstream. Compare
with the non-streaming traffic of the same minutes:

- other requests complete normally: the upstream is alive and this request was
  slow. Nothing to fix on the gateway.
- non-streaming requests end in `504` with code `wall_clock_exceeded` at 30
  seconds in the same window: the upstream is not answering. No alert fires
  for that condition today; the provider's status page and a direct request
  from inside the deployment confirm it.

**Fix**

For a slow request the change is on the consumer's side: a lower reasoning
effort, a smaller output, a smaller prompt. The deadline is not a setting. It
is a derived constant (`STREAM_TOTAL_DURATION_MS`, `src/config.ts`) held under
the outbound client's 300-second limit, and changing it means changing the
constant, its comment, its test and this entry in the same change.

**Planned, not current**

Today a stream the upstream accepts is not delivered to the client yet, so
this deadline only ends requests before any byte of a response is written.
After the first frame the response status will already be `200`, and the
deadline will end the stream from inside it: one error event carrying
`"gateway": {"terminal": "TOTAL_TIMEOUT"}`, then close. A response whose
content was fully delivered can still end that way, when the upstream's
terminating frame arrives after the deadline; retrying it blindly is the
consumer's choice and costs the operator a second execution. That cost is
reopened by a measured count of such endings, or by a decision to serve
reasoning effort `high`. This section becomes current in the change that
delivers streams to the client.
