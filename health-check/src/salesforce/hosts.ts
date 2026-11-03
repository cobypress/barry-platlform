// Where a token may be sent. Shared by the client, which sends the bearer token,
// and the OAuth transport, which sends codes, refresh tokens and secrets.

const SALESFORCE_HOSTS = [".salesforce.com", ".force.com", ".cloudforce.com", ".salesforce.mil", ".force.mil", ".cloudforce.mil"];

export function isSalesforceHost(hostname: string): boolean {
  return SALESFORCE_HOSTS.some((suffix) => hostname.endsWith(suffix));
}

/**
 * The token goes wherever this points, so it's parsed rather than prefix-checked:
 * "https://x.salesforce.com@evil.example" starts with https and is evil.example.
 * A test transport never leaves the process, so only the real one checks the host.
 */
export function instanceOrigin(instanceUrl: string, checkHost: boolean): string {
  let url: URL;
  try {
    url = new URL(instanceUrl);
  } catch {
    throw new Error("The instance URL isn't a valid URL");
  }
  if (url.protocol !== "https:") throw new Error("The instance URL must be https");
  if (url.username || url.password || url.search || url.hash || url.pathname !== "/") throw new Error("The instance URL must be a bare host");
  if (checkHost && !isSalesforceHost(url.hostname)) throw new Error("The instance URL isn't a Salesforce host");
  return url.origin;
}
