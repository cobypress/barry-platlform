export type OrgConnection = {
  instanceUrl: string;
  accessToken: string;
  /** Set this if you can. Otherwise connecting spends an API call to read it, which can stop at the budget. */
  orgId?: string;
  username?: string;
};

// How a run gets a token. The client doesn't care where it comes from, so
// Milestone 2's OAuth provider replaces the CLI one without touching anything else.
export interface ConnectionProvider {
  readonly kind: "sf-cli" | "oauth";
  connect(): Promise<OrgConnection>;
  /** Called once after a 401, then the request is retried. */
  refresh(): Promise<OrgConnection>;
}
