import http from "node:http";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import {
  createOpenAIClient,
  type OpenAIClient,
} from "../../src/upstream/openai.js";
import { listenEphemeral } from "../helpers/ephemeral-server.js";
import { wrapGlobalFetch } from "../helpers/global-fetch-wrapper.js";

// CONTRACT PIN — what a 3xx from the configured endpoint becomes, on the real
// adapter and the runtime's own fetch. The adapter asks for redirect
// "manual": the runtime must hand every 3xx back as a response, which the
// adapter turns into the 3xx outcome, and must send nothing to the host that
// Location names. If a runtime bump changes either half, this file fails
// before a 3xx is followed or relayed in production.
//
// Location names a second local server that records every request it
// receives: a followed hop is a recorded request, not an inferred one. The
// control arm proves the apparatus: the same scene, with a wrapper around the
// global fetch forcing redirect "follow", reaches that server once on each
// status the runtime follows.

const silentLog = { error: () => {}, info: () => {} };
const reqBody = { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] };

const adapterCalls = [
  {
    call: "buffered",
    run: (client: OpenAIClient, signal: AbortSignal): Promise<unknown> =>
      client.buffered(reqBody, signal, silentLog),
  },
  {
    call: "streaming",
    run: (client: OpenAIClient, signal: AbortSignal): Promise<unknown> =>
      client.streaming({ ...reqBody, stream: true }, signal, silentLog),
  },
];

const EVERY_3XX_STATUS = Array.from({ length: 100 }, (_, index) => 300 + index);
// The statuses the runtime's fetch treats as a redirect to follow.
const STATUSES_FETCH_FOLLOWS = [301, 302, 303, 307, 308];

const everyStatusOnBothCalls = adapterCalls.flatMap((adapterCall) =>
  EVERY_3XX_STATUS.map((status) => ({ ...adapterCall, status })),
);
const followedStatusesOnBothCalls = adapterCalls.flatMap((adapterCall) =>
  STATUSES_FETCH_FOLLOWS.map((status) => ({ ...adapterCall, status })),
);

describe("contract pin — a 3xx from the configured endpoint, real adapter and runtime fetch", () => {
  // Paths of the requests the second server received, in arrival order.
  const hopRequests: string[] = [];
  let hop: { port: number; close: () => Promise<void> };
  let upstream: { port: number; close: () => Promise<void> };
  let hopServer: http.Server;
  let upstreamServer: http.Server;

  const hopRequestsFor = (cell: string): number =>
    hopRequests.filter((path) => path === `/${cell}`).length;

  // The upstream answers the status its path names; with "with-location" the
  // answer carries a Location that names the second server and the cell.
  const upstreamBaseURL = (
    status: number,
    location: "with-location" | "without-location",
    cell: string,
  ): string => `http://127.0.0.1:${upstream.port}/${status}/${location}/${cell}`;

  beforeAll(async () => {
    hopServer = http.createServer((req, res) => {
      req.resume();
      hopRequests.push(req.url ?? "");
      res.writeHead(204);
      res.end();
    });
    hop = await listenEphemeral(hopServer);

    upstreamServer = http.createServer((req, res) => {
      req.resume();
      const [, status, location, cell] = (req.url ?? "").split("/");
      res.writeHead(
        Number(status),
        location === "with-location"
          ? { location: `http://127.0.0.1:${hop.port}/${cell ?? ""}` }
          : {},
      );
      res.end();
    });
    upstream = await listenEphemeral(upstreamServer);
  });

  afterAll(async () => {
    upstreamServer.closeAllConnections();
    hopServer.closeAllConnections();
    await upstream.close();
    await hop.close();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each(everyStatusOnBothCalls)(
    "$call call, $status with Location: the 3xx outcome, and nothing reaches the host Location names",
    async ({ call, run, status }) => {
      const cell = `${call}-${status}`;
      const client = createOpenAIClient({
        apiKey: "k",
        baseURL: upstreamBaseURL(status, "with-location", cell),
      });

      const outcome = await run(client, new AbortController().signal);

      expect(
        outcome,
        "every status from 300 to 399 reaches the adapter as a response and becomes the 3xx outcome carrying that status: the runtime must neither reject it nor follow it",
      ).toStrictEqual({ kind: "upstream_3xx", status });
      expect(
        hopRequestsFor(cell),
        "nothing may reach the host that Location names",
      ).toBe(0);
    },
  );

  it.each(adapterCalls)(
    "$call call, 302 without Location: the 3xx outcome, and the second server receives nothing",
    async ({ call, run }) => {
      const requestsBefore = hopRequests.length;
      const client = createOpenAIClient({
        apiKey: "k",
        baseURL: upstreamBaseURL(302, "without-location", `${call}-bare`),
      });

      const outcome = await run(client, new AbortController().signal);

      expect(
        outcome,
        "a 3xx is recognized by its status, with or without Location",
      ).toStrictEqual({ kind: "upstream_3xx", status: 302 });
      expect(
        hopRequests.length,
        "no request reached the second server during the cell",
      ).toBe(requestsBefore);
    },
  );

  it.each(followedStatusesOnBothCalls)(
    'CONTROL, $call call, $status: with redirect "follow" forced, the runtime reaches the host Location names once',
    async ({ call, run, status }) => {
      const cell = `follow-${call}-${status}`;
      const wrapper = wrapGlobalFetch("follow");
      const client = createOpenAIClient({
        apiKey: "k",
        baseURL: upstreamBaseURL(status, "with-location", cell),
      });

      await run(client, new AbortController().signal);

      expect(
        wrapper.responseStatuses,
        "control: the runtime followed the hop and handed back the second server's 204; without this the zero of the other cells could be a dead fixture",
      ).toStrictEqual([204]);
      expect(
        hopRequestsFor(cell),
        "control: the second server recorded the followed hop exactly once",
      ).toBe(1);
    },
  );
});
