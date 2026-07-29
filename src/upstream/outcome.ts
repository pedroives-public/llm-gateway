export type Outcome =
  | {
      kind: "upstream_error";
      status: number;
      body_raw: string;
      retry_after?: string;
    }
  | {
      kind: "undecodable";
    }
  | {
      kind: "network_failed";
      pre_send_proven: boolean;
      cause_code?: string;
      cause_name?: string;
    }
  | {
      kind: "redirect_blocked";
    }
  | {
      kind: "ok";
      status: number;
      body_parsed: unknown;
    }
  | {
      kind: "aborted";
      abort_kind: "response_size_cap" | "wall_clock_expired";
    };

export type ErrorOutcome = Exclude<Outcome, { kind: "ok" }>;
