# breaker_probe_backstop: a recovery probe never finished

This line raises no `operational_alert`, but it is written at error level
because only a gateway defect produces it. The responder arrives from an error
filter on the logs, or from tenants receiving `503
{"code":"circuit_breaker_open"}` for longer than usual.

**Log line**

```json
{"level":50,"event":"breaker_probe_backstop","attempt":7}
```

`attempt` is the recovery probe's per-process counter, the same value as in the
adjacent `breaker_probe_result` line, which records this attempt's `FAILURE`.

**Where it can happen**

Only while the breaker is HALF_OPEN, when a recovery probe attempt has not
voted within `PROBE_BACKSTOP_MS` (20 seconds, twice the probe's own 10-second
clock).

**Impact**

The breaker counts the attempt as `FAILURE`, returns to OPEN and runs the next
probe 60 seconds later. Every tenant keeps receiving `503` in the meantime. If
the defect is permanent, every probe ends here and the breaker never closes:
an outage with the upstream possibly healthy, until the process restarts.

**Meaning**

The probe has its own clock, which ends it after 10 seconds and writes
[`breaker_probe_clock_expired`](breaker-probe-clock-expired.md). The breaker's
backstop is a second, longer bound for the case where the probe code itself
stops making progress, so the breaker cannot stay HALF_OPEN forever. This line
means that bound was needed: the probe's own clock did not end the attempt.
The upstream connection of that attempt may stay open. The runtime's
300-second body timeout closes it only if the upstream stops sending; an
upstream that keeps sending bytes holds it open longer. If the attempt was
still waiting for the response head, an error-level
[`Unrecognized upstream rejection`](breaker-probe-cancel-abort.md) line
follows; it is a known false error.

**Diagnosis**

- One isolated line: keep the surrounding lines and the deployed version, and
  report it as a defect. The next probe usually succeeds or fails on its own.
- A line on every probe: the probe is stuck on every attempt. Compare with
  [`breaker_probe_clock_expired`](breaker-probe-clock-expired.md): if that line
  is absent, the probe's clock is not firing or not ending the attempt.

**Fix**

A restart clears the breaker (it starts CLOSED). The cause is in the gateway's
code, not in the provider or the configuration: open an issue with the lines.
