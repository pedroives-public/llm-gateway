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
