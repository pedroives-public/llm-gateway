import Fastify, { type FastifyInstance } from 'fastify';
import sensible from '@fastify/sensible';
import { healthRoute } from './routes/health.js';

interface BuildAppOptions {
  logger?: boolean | Record<string, unknown>;
}

export async function buildApp(options: BuildAppOptions = {}): Promise<FastifyInstance> {
  const app = Fastify({
    logger: options.logger ?? {
      level: process.env['LOG_LEVEL'] ?? 'info',
      transport:
        process.env['NODE_ENV'] !== 'production'
          ? { target: 'pino-pretty', options: { translateTime: 'HH:MM:ss.l', ignore: 'pid,hostname' } }
          : undefined,
    },
  });

  await app.register(sensible);
  await app.register(healthRoute);

  return app;
}
