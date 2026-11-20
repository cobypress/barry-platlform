export type OrgConnection = {
  instanceUrl: string;
  accessToken: string;
  /** Set this if you can. Otherwise connecting spends an API call to read it, which can stop at the budget. */
  orgId?: string;
  username?: string;
  /**
   * Set by a refresh that found newer tokens another process stored, rather
   * than getting its own (DECISIONS.md 631). That access token may have ended
   * too, so the client may ask for one more refresh before giving up.
   */
  fromStore?: boolean;
};

// How a run gets a token. The client doesn't care where it comes from, so
// Milestone 2's OAuth provider replaces the CLI one without touching anything else.
export interface ConnectionProvider {
  readonly kind: "sf-cli" | "oauth";
  connect(): Promise<OrgConnection>;
  /** Called once after a 401, then the request is retried. Twice if the first came back `fromStore`. */
  refresh(): Promise<OrgConnection>;
  /** Told when a request succeeds on a token a refresh returned `fromStore`. */
  accepted?(): void;
}
