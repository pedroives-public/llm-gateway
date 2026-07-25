import type { FastifyPluginAsync } from 'fastify';

// Public, unauthenticated route (deployment platform probes cannot
// authenticate). The body must stay liveness-only: version fingerprints the
// deployment for dependency CVEs; uptime reveals restart cadence.
export const healthRoute: FastifyPluginAsync = async (fastify) => {
  fastify.get('/health', async () => {
    return { ok: true };
  });
};
