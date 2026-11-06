import { XMLParser } from "fast-xml-parser";
import { isUnavailable } from "../salesforce/client.ts";
import { schemaExtractor } from "./configuration.ts";
import { readKeyset } from "./opportunities.ts";
import { daysBefore, soqlDateTime, unavailable, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";
import { licencesExtractor } from "./users.ts";

// Marketing Cloud Account Engagement, formerly Pardot (roadmap Stage 6,
// DECISIONS.md 261 to 274), read only from the Salesforce side: the fields its
// managed package (namespace pi) puts on Lead, and two of the org's settings.
// Never the Account Engagement API, which has its own route and consent.

export const PARDOT_NAMESPACE = "pi";
/** The Lead fields the checks read. pi__url__c is set on a lead that's synced with an Account Engagement prospect. */
export const PI_FIELDS = { url: "pi__url__c", score: "pi__score__c", grade: "pi__grade__c" } as const;
/** A synced lead nobody has touched this long after it was created has been left unworked. Our judgement. */
export const UNWORKED_DAYS = 14;
/**
 * The most open synced leads read. A 500-user org has about 28,000 handed over
 * in the last 90 days, so they're read in full (DECISIONS.md 385). More is a
 * part, and the checks say so.
 */
export const PARDOT_LEAD_ROWS = 100_000;

export type PardotPresence = { status: "installed"; leadFields: string[] } | { status: "absent" | "unknown"; reason: string };

export type CampaignInfluence = { customizable: boolean | null; fromAccountEngagement: boolean | null };

export type PardotSetup = { presence: PardotPresence; influence: Sourced<CampaignInfluence> };

/**
 * Installed when the package has a licence record or Lead has its url field.
 * The Lead describe is the schema step's, so this costs no call of its own.
 */
export function pardotPresence(ctx: ExtractContext): PardotPresence {
  const { packageLicenses } = ctx.get(licencesExtractor);
  const lead = ctx.get(schemaExtractor).Lead;
  const fields = lead?.status === "ok" ? lead.data.fields.map((f) => f.name) : [];
  const piFields = fields.filter((f) => f.toLowerCase().startsWith(`${PARDOT_NAMESPACE}__`));
  const licensed = packageLicenses.status === "ok" && packageLicenses.data.some((p) => (p.NamespacePrefix ?? "").toLowerCase() === PARDOT_NAMESPACE);
  if (licensed || piFields.some((f) => f.toLowerCase() === PI_FIELDS.url)) return { status: "installed", leadFields: piFields.map((f) => f.toLowerCase()) };
  if (lead?.status === "ok") return { status: "absent", reason: "Account Engagement (the Pardot package, namespace pi) isn't installed in this org, so there's nothing of it to check." };
  return { status: "unknown", reason: `We couldn't tell whether Account Engagement is installed: Lead couldn't be described${lead?.status === "unavailable" ? `: ${lead.reason}` : ""}.` };
}

const parser = new XMLParser({ removeNSPrefix: true, ignoreAttributes: true, parseTagValue: false });
/** CampaignSettings.enableCampaignInfluence2 and .enableB2bmaCampaignInfluence2 (Metadata API Developer Guide). Missing is unknown. */
export function parseInfluence(files: Record<string, string>): CampaignInfluence {
  let settings: Record<string, unknown> | undefined;
  for (const xml of Object.values(files)) {
    try {
      const root = (parser.parse(xml) as Record<string, unknown>).CampaignSettings;
      if (root && typeof root === "object") settings = root as Record<string, unknown>;
    } catch {
      // Unreadable: left unknown.
    }
  }
  const read = (field: string): boolean | null => (settings?.[field] === "true" ? true : settings?.[field] === "false" ? false : null);
  return { customizable: read("enableCampaignInfluence2"), fromAccountEngagement: read("enableB2bmaCampaignInfluence2") };
}

export const INFLUENCE_SOURCE: SourceInfo = { kind: "metadata", path: "Settings: Campaign" };

export const pardotSetupExtractor: Extractor<PardotSetup> = {
  id: "pardot-setup",
  version: 1,
  async run(ctx) {
    const presence = pardotPresence(ctx);
    if (presence.status !== "installed") return { presence, influence: { status: "unavailable", reason: presence.reason, source: INFLUENCE_SOURCE } };
    try {
      const files = await ctx.client.retrieveMetadata([{ name: "Settings", members: ["Campaign"] }]);
      return { presence, influence: { status: "ok", data: parseInfluence(files), source: INFLUENCE_SOURCE, truncated: false } };
    } catch (err) {
      // A read-only connection can't use the Metadata API (DECISIONS.md 73).
      if (!isUnavailable(err)) throw err;
      return { presence, influence: unavailable(INFLUENCE_SOURCE, err) };
    }
  },
};

export type SyncedLeadRow = { Id: string; Name: string; OwnerId: string; CreatedDate: string; LastActivityDate: string | null; LeadSource: string | null };
export type PardotLeads = {
  /** Synced leads created in the window: all, with no grade, and with no score. */
  counts: Record<"synced" | "ungraded" | "unscored", Sourced<number>>;
  /** Open synced leads created in the window. */
  open: Sourced<SyncedLeadRow[]>;
};

const notRead = (reason: string, query: string): Sourced<never> => ({ status: "unavailable", reason, source: { kind: "soql", api: "rest", query } });

// Records, so a set-up review never reads them.
export const pardotLeadsExtractor: Extractor<PardotLeads> = {
  id: "pardot-leads",
  version: 2,
  records: true,
  partialKeys: ["pardot-open-leads"],
  async run(ctx) {
    const { presence } = ctx.get(pardotSetupExtractor);
    const window = `${PI_FIELDS.url} != null AND CreatedDate >= ${soqlDateTime(ctx.windowStart)}`;
    const queries = {
      synced: `SELECT COUNT() FROM Lead WHERE ${window}`,
      ungraded: `SELECT COUNT() FROM Lead WHERE ${window} AND ${PI_FIELDS.grade} = null`,
      unscored: `SELECT COUNT() FROM Lead WHERE ${window} AND (${PI_FIELDS.score} = null OR ${PI_FIELDS.score} = 0)`,
    };
    const openWhere = `${window} AND IsConverted = false`;
    const select = "Id, Name, OwnerId, CreatedDate, LastActivityDate, LeadSource";
    const openSoql = `SELECT ${select} FROM Lead WHERE ${openWhere}`;
    if (presence.status !== "installed") {
      return { counts: { synced: notRead(presence.reason, queries.synced), ungraded: notRead(presence.reason, queries.ungraded), unscored: notRead(presence.reason, queries.unscored) }, open: notRead(presence.reason, openSoql) };
    }
    // Each field is checked against the org's own Lead describe, so a different name is a named gap, not a failed run (QUESTIONS.md).
    const missing = Object.values(PI_FIELDS).filter((f) => !presence.leadFields.includes(f));
    if (missing.length) {
      const reason = `Lead.${missing.join(" and Lead.")} ${missing.length === 1 ? "isn't" : "aren't"} in this org, so Account Engagement's leads can't be read.`;
      return { counts: { synced: notRead(reason, queries.synced), ungraded: notRead(reason, queries.ungraded), unscored: notRead(reason, queries.unscored) }, open: notRead(reason, openSoql) };
    }
    const keys = Object.keys(queries) as Array<keyof typeof queries>;
    const results = await ctx.client.queryBatch(keys.map((k) => queries[k]));
    const counts = Object.fromEntries(
      keys.map((key, i) => {
        const r = results[i]!;
        const source: SourceInfo = { kind: "soql", api: "rest", query: queries[key] };
        return [key, r.ok ? { status: "ok", data: r.result.totalSize, source, truncated: false } : { status: "unavailable", reason: r.error.message, source }];
      }),
    ) as PardotLeads["counts"];
    let open: Sourced<SyncedLeadRow[]>;
    const source: SourceInfo = { kind: "soql", api: "rest", query: openSoql };
    try {
      const rows = await readKeyset<SyncedLeadRow>(ctx, "pardot-open-leads", select, "Lead", openWhere, PARDOT_LEAD_ROWS);
      open = { status: "ok", data: rows.slice(0, PARDOT_LEAD_ROWS), source, truncated: rows.length >= PARDOT_LEAD_ROWS };
    } catch (err) {
      open = unavailable(source, err);
    }
    return { counts, open };
  },
};

/** When a synced lead has been left long enough to count as unworked. */
export const unworkedBefore = (now: Date): Date => daysBefore(now, UNWORKED_DAYS);
