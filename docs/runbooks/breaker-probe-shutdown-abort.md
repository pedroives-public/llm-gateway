# Unrecognized upstream rejection at shutdown: the app cancelled its recovery probe

This line is written at error level and raises no `operational_alert`. With
the shape below, written while the process is stopping, it is not an upstream
fault: the gateway cancelled its own recovery probe. It is a known false
error.

**Log line**

```json
{"level":50,"cause_code":"UNKNOWN","cause_name":"UNKNOWN","err_name":"AbortError","abort_identity":true,"msg":"Unrecognized upstream rejection"}
```

`err_name` is `AbortError` and `abort_identity` is `true`: the rejection is
the cancellation of the request itself, not a network error.

**Where it can happen**

Only when the app closes (a deploy, a restart, a machine stop) while the
breaker's recovery probe is still waiting for the upstream's response head.
The probe runs only while the breaker is HALF_OPEN, for at most its 10-second
clock, once every 60 seconds the breaker spends OPEN. The breaker's backstop
cancels a stuck probe the same way, so the line can also follow a
[`breaker_probe_backstop`](breaker-probe-backstop.md) line.

**Impact**

None of its own. On a close, the breaker is stopping and discards the probe's
vote. After a backstop, the backstop has already voted `FAILURE`, and its own
entry describes the impact.

**Meaning**

The close cancels the probe without a reason the gateway's rejection
recognizer knows, so the upstream adapter reports the cancellation as an
unrecognized rejection before handing it back. The probe recognizes its own
cancellation and writes nothing else.

**Diagnosis**

- The shape above, seconds before the process stopped, or right after a
  `breaker_probe_backstop` line: no action.
- The same message outside a shutdown, or with another `err_name` or with
  `abort_identity: false`: this is not the case described here. Keep the
  surrounding lines and the deployed version, and report it as a defect.

**Fix**

No action for this case.
