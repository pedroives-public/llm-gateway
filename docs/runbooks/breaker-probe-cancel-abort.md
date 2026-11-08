# Unrecognized upstream rejection with AbortError: the gateway cancelled its recovery probe

This line is written at error level and raises no `operational_alert`. With
the shape below, right after a `breaker_probe_backstop` line, it is not an
upstream fault: the gateway cancelled its own recovery probe. It is a known
false error.

**Log line**

```json
{"level":50,"cause_code":"UNKNOWN","cause_name":"UNKNOWN","err_name":"AbortError","abort_identity":true,"msg":"Unrecognized upstream rejection"}
```

`err_name` is `AbortError` and `abort_identity` is `true`: the rejection is
the cancellation of the request itself, not a network error.

**Where it can happen**

When the breaker's backstop cancels a recovery probe that is still waiting for
the upstream's response head, so it follows a
[`breaker_probe_backstop`](breaker-probe-backstop.md) line. The app's close
hook cancels a probe the same way, but a production process today stops
without running that hook, so in production the backstop is the path.

**Impact**

None of its own. The backstop has already voted `FAILURE`, and its entry
describes the impact.

**Meaning**

The backstop cancels the probe without a reason the gateway's rejection
recognizer knows, so the upstream adapter reports the cancellation as an
unrecognized rejection before handing it back. The probe recognizes its own
cancellation and writes nothing else.

**Diagnosis**

- The shape above, right after a `breaker_probe_backstop` line: follow that
  entry; this line adds nothing to it.
- The same message without a backstop line just before it, or with another
  `err_name` or with `abort_identity: false`: this is not the case described
  here. Keep the surrounding lines and the deployed version, and report it as
  a defect.

**Fix**

Nothing beyond the backstop entry's own steps.
