// Hard-timeout helpers. Single abort point by construction: controller and
// typed abort funnel are module-private and must never be exported —
// consumers get only the read-only signal plus an idempotent clear().
// Rejection recognition matches on the abort reason's `kind`.

export type ArmedTimeout = {
  signal: AbortSignal;
  clear: () => void;
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
): ArmedTimeout {
  const controller = new AbortController();

  const abortWith = (kind: "total_timeout") => {
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

  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  };
}
