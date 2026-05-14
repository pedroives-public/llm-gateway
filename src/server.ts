import { buildApp } from './app.js';

const parsedPort = Number(process.env['PORT']);
const PORT = Number.isFinite(parsedPort) ? parsedPort : 3000;
const HOST = process.env['HOST'] ?? '0.0.0.0';

async function start(): Promise<void> {
  const app = await buildApp();

  try {
    await app.listen({ port: PORT, host: HOST });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();
