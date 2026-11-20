import type { OAuthClient } from "../salesforce/oauth.ts";
import type { WorkerDeps } from "./deps.ts";

// The worker's Salesforce apps (DECISIONS.md 616): the package app, the
// External Client App customers install, and the sign-in app, a classic
// connected app an admin can authorise with nothing installed. A code or a
// refresh token belongs to the app that issued it, so each connection uses
// that app and no other: an id the worker has no app for gets null, never the
// other app. Only exact client ids match.

export type WorkerApps = Pick<WorkerDeps, "oauthClient" | "signinClient">;

export function appFor(clientId: string, deps: WorkerApps): OAuthClient | null {
  for (const app of [deps.oauthClient, deps.signinClient]) if (app && app.clientId === clientId) return app;
  return null;
}

/** The app a link's trip to Salesforce used. A pending authorisation from an older console names none: the package app. */
export function appForPending(pending: { clientId?: string }, deps: WorkerApps): OAuthClient | null {
  return pending.clientId === undefined ? deps.oauthClient : appFor(pending.clientId, deps);
}

export const APP_NOT_SET_UP_AT_CONNECT = "Barometer's Salesforce app for this link isn't set up on our side, so we didn't sign you in and read nothing. Ask your Black Cloud consultant.";

export const appNotSetUp = (name: string): string =>
  `The connection "${name}" was made through a Salesforce app this worker is no longer set up for, so nothing was read. Set that app up on the worker again, or send the customer's admin a new connect link.`;

/** The run's reason when Salesforce has ended the connection (DECISIONS.md 617). The console matches its first words. */
export const NEEDS_RECONNECTION = "Needs reconnection:";

export const needsReconnection = (name: string): string =>
  `${NEEDS_RECONNECTION} Salesforce no longer accepts the connection "${name}". A connection ends after 30 days unused, or when someone in the org revokes it. Send the customer's admin a connect link to sign in again.`;
