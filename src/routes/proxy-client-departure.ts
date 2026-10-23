import type { ServerResponse } from "node:http";
import type { TotalDurationTimeout } from "../reliability/timeouts.js";

type AbortForClientGone = TotalDurationTimeout["abortForClientGoneIfUndecided"];

export function observeClientDeparture(
  raw: ServerResponse,
  abortForClientGoneIfUndecided: AbortForClientGone,
): void {
  raw.once("close", () => {
    if (!raw.writableFinished) {
      abortForClientGoneIfUndecided();
    }
  });

  if (raw.destroyed) {
    abortForClientGoneIfUndecided();
  }
}
