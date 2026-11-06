import { afterEach, describe, it, expect } from "vitest";
import { PROBE_ALLOWED_HEADER_NAMES } from "./helpers/probe-header-names.js";
import {
  startRealAppScene,
  type RealAppScene,
} from "./helpers/real-app-breaker-scene.js";

describe("real app — breaker recovery probe", () => {
  let scene: RealAppScene | undefined;

  afterEach(async () => {
    await scene?.close();
    scene = undefined;
  });

  it("a tenant request during HALF_OPEN never takes the probe's place", async () => {
    scene = await startRealAppScene();
    await scene.openBreaker();
    await scene.elapseCooldown();
    const tenantsBeforeHalfOpen = scene.tenantRequests().length;

    const responses = await Promise.all([
      scene.send(),
      scene.send(),
      scene.send(),
    ]);

    expect(
      responses.map((response) => response.statusCode),
      "during HALF_OPEN every tenant request is refused: the probe is the only request the breaker lets through",
    ).toEqual([503, 503, 503]);

    expect(
      responses.map(
        (response) => response.json<{ error: { code: string } }>().error.code,
      ),
      "the refusal comes from the breaker, not from another 503",
    ).toEqual([
      "circuit_breaker_open",
      "circuit_breaker_open",
      "circuit_breaker_open",
    ]);

    expect(
      scene.tenantRequests(),
      "no tenant request may reach the upstream while the probe holds the half-open slot",
    ).toHaveLength(tenantsBeforeHalfOpen);
    expect(
      scene.probeRequests(),
      "exactly one probe is in flight; no tenant request became a second one",
    ).toHaveLength(1);
  });

  it("the probe carries nothing from tenant headers or the tenant key", async () => {
    scene = await startRealAppScene();

    const canary = "tenant-canary-6f2b";
    const tenantKey = `lkey_${canary.padEnd(43, "A")}`;

    await scene.openBreaker({
      headers: {
        authorization: `Bearer ${tenantKey}`,
        "x-tenant-canary": canary,
        "user-agent": `tenant-client-${canary}`,
      },
    });
    await scene.elapseCooldown();

    // Positive control: tenant headers are never forwarded upstream, so the
    // proof that the canaries were inside the app is that the canary requests
    // passed auth and reached the upstream.
    expect(
      scene.tenantRequests(),
      "the canary requests must reach the upstream, or the rulers below see nothing",
    ).toHaveLength(5);

    const probe = scene.probeRequests()[0];
    if (!probe) throw new Error("the probe did not reach the upstream");

    expect(probe.method, "the probe is an inference request").toBe("POST");
    expect(
      probe.url,
      "the probe's endpoint comes from deployment config alone",
    ).toBe("/v1/chat/completions");
    expect(
      probe.headers.authorization,
      "the probe's credential comes from deployment config, never from the tenant's key",
    ).toBe("Bearer sk-test-placeholder");
    expect(
      JSON.parse(probe.body),
      "the probe's body is fixed: the configured model and nothing from a tenant",
    ).toEqual({
      model: "probe-model",
      messages: [{ role: "user", content: "ping" }],
      stream: true,
      max_tokens: 1,
    });
    expect(
      Object.keys(probe.headers).filter(
        (name) => !PROBE_ALLOWED_HEADER_NAMES.has(name),
      ),
      "the probe carries only the headers the adapter and the runtime add",
    ).toEqual([]);

    // Explicit net over what the literals above do not fix: the values of the
    // allowed headers, where a tenant value (such as its user-agent) would
    // hide under an allowed name.
    expect(
      JSON.stringify(probe),
      "no tenant bytes may reach the probe's request",
    ).not.toContain(canary);
  });
});
