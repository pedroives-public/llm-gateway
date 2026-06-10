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
    }
  | {
      kind: "ok";
      status: number;
      body_parsed: unknown;
    };

export type ErrorOutcome = Exclude<Outcome, { kind: "ok" }>;
