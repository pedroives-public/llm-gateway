import type { FastifyPluginAsync } from 'fastify';
import packageJson from '../../package.json' with { type: 'json' };

export const healthRoute: FastifyPluginAsync = async (fastify) => {
  fastify.get('/health', async () => {
    return {
      ok: true,
      version: packageJson.version,
      uptime: process.uptime(),
    };
  });
};
