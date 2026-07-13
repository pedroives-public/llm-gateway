import type http from "node:http";
import type { AddressInfo } from "node:net";

// Binds the server to an ephemeral loopback port; close() resolves only after
// the handle is released, so finally blocks cannot leak sockets across tests.
export async function listenEphemeral(
  server: http.Server,
): Promise<{ port: number; close: () => Promise<void> }> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    port,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  };
}
