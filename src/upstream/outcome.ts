export type Outcome =
  | {
      kind: "upstream_error";
      status: number;
      body_raw: string;
    }
  | {
      kind: "undecodable";
    }
  | {
      kind: "network_failed";
    }
  | {
      kind: "ok";
      status: number;
      body_parsed: unknown;
    };

export type ErrorOutcome = Exclude<Outcome, { kind: "ok" }>;
