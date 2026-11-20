import { instanceOrigin } from "../salesforce/hosts.ts";

// The org's own address, for the customer portal's links into Setup
// (DECISIONS.md 626). Only a My Domain host is kept: the customer's own,
// never a login host or an instance server, and never with a scheme, path,
// port or query. The pattern is the console's check constraint on
// HealthCheckConnectLink."instanceHost" word for word, so a host we write is
// never refused by it. Sandboxes (name--box.sandbox.my.salesforce.com),
// Developer Editions (name-dev-ed.develop.my.salesforce.com) and scratch orgs
// match; Government Cloud and Hyperforce's other forms don't, and are in
// QUESTIONS.md.

export const MY_DOMAIN_HOST = /^[a-z0-9-]+(\.[a-z0-9-]+)*\.my\.salesforce\.com$/;

export const isMyDomainHost = (host: unknown): host is string => typeof host === "string" && host.length <= 253 && MY_DOMAIN_HOST.test(host);

/**
 * The bare My Domain host of an instance URL, lower case, or null for anything
 * else: a URL that isn't one, one `instanceOrigin` refuses, one with a port, or
 * a host that isn't a My Domain. Never throws, so it can't fail a connect.
 */
export function myDomainHost(instanceUrl: unknown): string | null {
  if (typeof instanceUrl !== "string") return null;
  let url: URL;
  try {
    url = new URL(instanceOrigin(instanceUrl, true));
  } catch {
    return null;
  }
  if (url.port !== "") return null;
  const host = url.hostname.toLowerCase();
  return isMyDomainHost(host) ? host : null;
}
