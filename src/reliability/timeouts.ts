// Hard-timeout helpers. Single abort point by construction: controller and
// typed abort funnel are module-private and must never be exported. The
// wall-clock helper hands out only the read-only signal plus an idempotent
// clear(). The total-duration helper adds two narrowed abort capabilities,
// each meant for the one piece of code that needs it: the client-departure
// observer's does nothing once a terminal is decided (a late departure lost
// that race and must not become the signal's first reason), and the terminal
// block's throws while no terminal is decided (aborting before deciding is a
// defect, so it fails loudly instead of being skipped).
// Rejection recognition matches on the abort reason's `kind`.

class TerminalAbortBeforeDecisionError extends Error {
  constructor() {
    super("cannot abort for a terminal before the terminal is decided");
    this.name = "TerminalAbortBeforeDecisionError";
  }
}

type AbortKind =
  | "wall_clock_expired"
  | "total_timeout"
  | "client_gone"
  | "parser_buffer_cap"
  | "response_size_cap";

// The kinds a terminal block may abort with after deciding its terminal.
type TerminalAbortKind = "total_timeout" | "client_gone" | "parser_buffer_cap";

export type ArmedTimeout = {
  signal: AbortSignal;
  clear: () => void;
};

export type TotalDurationTimeout = {
  signal: AbortSignal;
  clear: () => void;
  abortForClientGoneIfUndecided: () => void;
  abortForTerminalAfterDecision: (kind: TerminalAbortKind) => void;
};

// Non-streaming deadline: armed at handler entry, spans the whole request
// envelope including any retry wait.
export function armWallClockTimeout(ms: number): ArmedTimeout {
  const controller = new AbortController();

  // Not a redundant wrapper: abort(reason?: any) is unchecked, so the
  // funnel's narrow parameter makes a mistyped kind a compile error.
  const abortWith = (kind: "wall_clock_expired") => {
    controller.abort({ kind });
  };

  const timer = setTimeout(() => {
    abortWith("wall_clock_expired");
  }, ms);

  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  };
}

// Streaming deadline. `deadlineAt` is the absolute instant (epoch ms) computed
// once at handler entry; no retry moves it. The timer only wakes. Each firing
// first asks `isTerminalDecided` (a getter: read at the firing, never copied at
// arming) and does nothing if a terminal is decided. Then it reads the deadline
// value, because a timer can fire while Date.now() is still short of it (Node
// starts timers from the event loop's cached time): short of the value, it arms
// one timer for the remainder instead of aborting. clear() disarms whichever
// timer is armed when it is called.
export function armTotalDurationTimeout(
  deadlineAt: number,
  isTerminalDecided: () => boolean,
): TotalDurationTimeout {
  const controller = new AbortController();

  const abortWith = (kind: AbortKind) => {
    controller.abort({ kind });
  };

  let timer: ReturnType<typeof setTimeout>;
  const scheduler = () => {
    const remainingMs = deadlineAt - Date.now();

    timer = setTimeout(() => {
      if (isTerminalDecided()) {
        return;
      }

      if (Date.now() >= deadlineAt) {
        abortWith("total_timeout");
        return;
      }

      // Fired short of the deadline value: wait out the remainder.
      scheduler();
    }, remainingMs);
  };

  scheduler();

  const abortForClientGoneIfUndecided = (): void => {
    if (isTerminalDecided()) {
      return;
    }
    abortWith("client_gone");
  };

  const abortForTerminalAfterDecision = (kind: TerminalAbortKind): void => {
    if (!isTerminalDecided()) {
      throw new TerminalAbortBeforeDecisionError();
    }
    abortWith(kind);
  };

  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
    abortForClientGoneIfUndecided,
    abortForTerminalAfterDecision,
  };
}
