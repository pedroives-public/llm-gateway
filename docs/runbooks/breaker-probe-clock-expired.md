# breaker_probe_clock_expired: the recovery probe outlived its clock

This line raises no `operational_alert`. The responder arrives from a run of
`breaker_probe_result` lines with outcome `FAILURE` while tenants receive
`503 {"code":"circuit_breaker_open"}`.

**Log line**

```json
{"level":30,"event":"breaker_probe_clock_expired","req_id":"breaker_probe","upstream_reply":"not_finished"}
```

No tenant request carries the probe, so `req_id` is the fixed value
`breaker_probe`. The adjacent `breaker_probe_result` line records the attempt
and its `FAILURE`.

**Where it can happen**

Only while the breaker is HALF_OPEN: once per recovery probe, after the breaker
opened and its 60-second cooldown elapsed.

**Impact**

The probe counts as `FAILURE`. The breaker returns to OPEN and the next probe
runs 60 seconds later. Every tenant keeps receiving `503` in the meantime. No
tenant request is involved in the probe itself.

**Meaning**

The gateway checks whether the upstream recovered with a small synthetic
request of its own: one output token, fixed message, the model from the
deployment setting `BREAKER_PROBE_MODEL`. The reply must reach its end
(`[DONE]`) within `PROBE_CLOCK_MS`, 10 seconds; in production the probe's
reply took at most 4.6 seconds across two measurements of 20 requests each.
This line means the clock decided first: the probe voted `FAILURE` and
cancelled its request, which normally also closes the connection to the
upstream. If a [`breaker_probe_backstop`](breaker-probe-backstop.md) line
follows for the same attempt, the cancel did not end the request, and the
connection may still be open.

`upstream_reply` says how far the upstream got:

- `not_started`: no stream started within 10 seconds. Either no response
  head arrived, or the head carried an error status and its body was still
  arriving.
- `not_finished`: the upstream started replying but did not finish a
  one-token reply within 10 seconds.

**Diagnosis**

Read the value over the last few probes (one every 70 seconds or so):

- `not_started`, repeated: the provider is down, unreachable from the
  deployment, or answering an error whose body arrives slowly. Send the
  probe's own request from inside the deployment (the diagnosis request in
  [`upstream_probe_rejected`](README.md#upstream_probe_rejected)): no answer
  means down or unreachable, and an error status names the cause. Check the
  provider's status page too.
- `not_finished`, repeated: the provider answers but is degraded, or something
  between the gateway and the provider holds the connection open (for example
  a proxy that keeps sending keep-alive comments). The same direct request
  shows how long the reply takes to end.
- `not_finished`, repeated, while the direct request finishes in about a
  second: suspect the gateway's own reading of the reply. Keep the lines and
  the deployed version for the investigation.

**Fix**

Provider side: follow the provider's status; the breaker closes on its own at
the first probe that reaches `[DONE]` in time. The clock is a derived constant
(`PROBE_CLOCK_MS`, `src/config.ts`): changing it means changing the constant,
its comment, its tests and this entry in the same change.
