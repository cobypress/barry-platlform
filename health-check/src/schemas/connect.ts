import { z } from "zod";
import { Timestamp } from "./common.ts";

// The one-time connect link a consultant sends to a customer's admin. The
// console stores these in Milestone 5. Only a hash of the link's secret is kept,
// so a leaked table can't be turned back into working links.

const Hash = z.string().regex(/^[A-Za-z0-9_-]{43}$/, "A base64url SHA-256");

export const ConnectLink = z.object({
  id: z.string().uuid(),
  tokenHash: Hash,
  customerRef: z.string().min(1),
  /** What the connection is stored as once the admin finishes. */
  connectionName: z.string().regex(/^[a-z0-9][a-z0-9-]{0,62}$/),
  /** login.salesforce.com, test.salesforce.com, or the customer's My Domain. */
  loginUrl: z.string().url(),
  createdBy: z.string().min(1),
  createdAt: Timestamp,
  expiresAt: Timestamp,
  usedAt: Timestamp.nullable(),
  revokedAt: Timestamp.nullable(),
});
export type ConnectLink = z.infer<typeof ConnectLink>;

/** Between the admin leaving for Salesforce and coming back. Holds the PKCE verifier, so it's stored encrypted. */
export const PendingAuthorisation = z.object({
  linkId: z.string().uuid(),
  stateHash: Hash,
  codeVerifier: z.string().min(43).max(128),
  createdAt: Timestamp,
  expiresAt: Timestamp,
});
export type PendingAuthorisation = z.infer<typeof PendingAuthorisation>;
