import { NotInOrgError, type GatedRead } from "../salesforce/client.ts";
import { editionNote, editionPointer, type EditionLack } from "../salesforce/refusal-words.ts";
import type { CapabilityStatus, OrgCapability, OrgProfile, SkippedRead } from "../schemas/index.ts";
import { orgExtractor, type ObjectSummary } from "./configuration.ts";
import { readName, type Extractor, type Sourced } from "./types.ts";

// Know the org before reading it (DECISIONS.md 710). The run used to find out
// what an org lacks by asking and being refused; now its first steps build a
// profile from what they already read (the Organization row), and a read the
// profile says can't work is never sent. Its checks are not measured, and say
// the edition is why.
//
// Absent is only ever a fact Salesforce documents about an edition: the
// audit of every read (orchestration/health-check/roadmap/ORG-COMPATIBILITY-AUDIT.md)
// records each, with the guide it came from. Anything less certain, an edition
// string we don't recognise or a field the row didn't have, is unknown, and
// unknown is tried as before: a refusal then costs its one check (702).

/** The edition Salesforce's Organization.OrganizationType names for Professional Edition, as the Apex step has matched it since before 710. */
export const PROFESSIONAL = "Professional Edition";

/** The editions Salesforce's Metadata API guide says can use it ("Supported Salesforce Editions"). */
const METADATA_EDITIONS: ReadonlySet<string> = new Set(["Enterprise Edition", "Unlimited Edition", "Performance Edition", "Developer Edition"]);

/** What the reads depend on, that the profile can know before they're made. */
export const GATED = ["metadata_api", "own_apex", "s_controls", "sites"] as const;
export type GatedCapability = (typeof GATED)[number];

/** Learned during the run, for the record only: no read is skipped for these. */
export const RECORDED = ["multi_currency", "record_types"] as const;

/**
 * Each gated capability, and why Professional Edition doesn't have it. The
 * sources are Salesforce's own guides, as the audit recorded them.
 */
const IN_EDITION: Record<GatedCapability, { professional: string; other: (edition: string) => { status: CapabilityStatus; from: string } }> = {
  metadata_api: {
    // META "Supported Salesforce Editions" and "Metadata API Access for Professional Edition": only through an
    // ISV app that has passed security review, which Barometer's unlisted package hasn't (DECISIONS.md 79).
    professional: "Professional Edition has no Metadata API, except for apps that have passed Salesforce's security review, which Barometer hasn't.",
    other: (edition) =>
      METADATA_EDITIONS.has(edition)
        ? { status: "present", from: `${edition} includes the Metadata API; whether the connected user may use it is learned by reading.` }
        : { status: "unknown", from: `Whether ${edition} includes the Metadata API isn't one of the facts we hold, so it's read.` },
  },
  own_apex: {
    // ISV "Using Apex in Group and Professional Editions": Apex isn't supported there, except in managed packages.
    professional: "Professional Edition has no Apex of the org's own; only installed packages' Apex can run there.",
    other: () => ({ status: "unknown", from: "Read: the org's own Apex classes and triggers." }),
  },
  s_controls: {
    // TOOL "Scontrol": Enterprise, Developer or Unlimited Edition only, with s-controls switched on.
    professional: "Professional Edition has no s-controls.",
    other: () => ({ status: "unknown", from: "Read: s-controls must be switched on even where the edition has them." }),
  },
  sites: {
    // ISV "Features in Group and Professional Editions": Sites, No.
    professional: "Professional Edition has no Sites.",
    other: () => ({ status: "unknown", from: "Read: the org's sites." }),
  },
};

/**
 * The one table of reads the profile can rule out (DECISIONS.md 710), beside
 * the steps that make them. A read matching a row whose capability is absent
 * isn't sent: the client throws a NotInOrgError in its place, with the code
 * Salesforce would answer, so each step goes on as it already does for that
 * answer, and the customer reads the row's sentence.
 */
export type ReadNeed = {
  capability: GatedCapability;
  /** What isn't read, named as run.json's skipped reads name it. */
  read: string;
  /** The steps that make this read. A test holds this to what a run does. */
  steps: readonly string[];
  matches: (read: GatedRead) => boolean;
  /** Salesforce's own answer for it, so each step handles it as it does that: INSUFFICIENT_ACCESS takes the Tooling and Analytics routes (DECISIONS.md 73). */
  code: "INSUFFICIENT_ACCESS" | "INVALID_TYPE";
  sentence: string;
};

const fromObject = (read: GatedRead): string | null => (read.kind === "query" ? readName({ kind: "soql", api: read.api, query: read.soql }) : null);

export const READ_NEEDS: readonly ReadNeed[] = [
  {
    capability: "metadata_api",
    read: "Metadata API retrieve",
    // The metadata step then reads through Tooling, and report-definitions through the Analytics API; the other three have no other route.
    steps: ["metadata", "report-definitions", "service-escalation-rules", "ai-signals", "pardot-setup"],
    matches: (read) => read.kind === "retrieve",
    code: "INSUFFICIENT_ACCESS",
    sentence: editionPointer("the Metadata API"),
  },
  {
    capability: "own_apex",
    read: "ApexClass and ApexTrigger (the org's own)",
    steps: ["apex", "code-lint"],
    // The org's own only: an installed package's Apex can run in Professional Edition.
    matches: (read) => read.kind === "query" && read.api === "rest" && ["ApexClass", "ApexTrigger"].includes(fromObject(read) ?? "") && /\bNamespacePrefix\s*=\s*null\b/i.test(read.soql),
    code: "INVALID_TYPE",
    sentence: editionPointer("Apex"),
  },
  {
    capability: "s_controls",
    read: "Scontrol",
    steps: ["classic"],
    matches: (read) => fromObject(read) === "Scontrol",
    code: "INVALID_TYPE",
    sentence: editionPointer("s-controls"),
  },
  {
    capability: "sites",
    read: "Site",
    // INVALID_TYPE is what the run-as step already takes as "no sites, so nothing runs through one".
    steps: ["run-as"],
    matches: (read) => fromObject(read) === "Site",
    code: "INVALID_TYPE",
    sentence: editionPointer("Sites"),
  },
];

/** What each gated capability is called in the run's note (DECISIONS.md 710). */
const LACKS: Record<GatedCapability, EditionLack> = { metadata_api: "the Metadata API", own_apex: "Apex", s_controls: "s-controls", sites: "Sites" };

/**
 * The run's one note about its edition, or null for an org the edition left
 * nothing out of: an org with everything gets none, so what it reads is as before.
 */
export function editionNoteOf(profile: OrgProfile | undefined): string | null {
  if (!profile) return null;
  const absent = profile.capabilities.filter((c) => c.status === "absent" && (GATED as readonly string[]).includes(c.id));
  return editionNote(absent.map((c) => LACKS[c.id as GatedCapability]));
}

/** The capabilities the org's edition settles, from the Organization row the org step read. */
export function profileFromOrg(org: { status: string; data?: Array<{ OrganizationType?: unknown }> } | undefined): OrgProfile {
  const raw = org?.status === "ok" ? org.data?.[0]?.OrganizationType : undefined;
  // Only a name: a missing or empty field is unknown, never an edition.
  const edition = typeof raw === "string" && raw.trim() !== "" ? raw.trim() : null;
  const capabilities: OrgCapability[] = GATED.map((id) => {
    if (edition === null) return { id, status: "unknown", from: "The org's edition couldn't be read, so this is read." };
    if (edition === PROFESSIONAL) return { id, status: "absent", from: IN_EDITION[id].professional };
    return { id, ...IN_EDITION[id].other(edition) };
  });
  for (const id of RECORDED) capabilities.push({ id, status: "unknown", from: "Learned as the run reads." });
  return { edition, capabilities, skipped: [] };
}

/** The first steps' profile, before any read it can rule out. No call of its own. */
export const orgProfileExtractor: Extractor<OrgProfile> = {
  id: "org-profile",
  version: 1,
  run: async (ctx) => profileFromOrg(ctx.get(orgExtractor)),
};

/**
 * The run's gate for one step: the error to throw in place of a read the
 * profile rules out, noting it, or null to send the read. Only `absent`
 * skips; `present` and `unknown` are read.
 */
export function gateFor(profile: OrgProfile, step: string, note: (skipped: SkippedRead) => void): (read: GatedRead) => NotInOrgError | null {
  const absent = new Set(profile.capabilities.filter((c) => c.status === "absent").map((c) => c.id));
  return (read) => {
    const need = READ_NEEDS.find((n) => absent.has(n.capability) && n.matches(read));
    if (!need) return null;
    note({ step, read: need.read, capability: need.capability });
    return new NotInOrgError(need.code, need.capability, need.sentence);
  };
}

/**
 * What the run learned after the profile was built, for the record: whether
 * deals carry a currency each (a field only multi-currency orgs have, Object
 * Reference "Frequently Occurring Fields"), and whether the core objects have
 * record types. Nothing is skipped for these, and anything not read stays unknown.
 */
export function completeProfile(profile: OrgProfile, schema: Record<string, Sourced<ObjectSummary>> | undefined): OrgProfile {
  const set = (id: string, status: CapabilityStatus, from: string): void => {
    const at = profile.capabilities.findIndex((c) => c.id === id);
    if (at !== -1) profile.capabilities[at] = { id, status, from };
  };
  const deals = schema?.Opportunity;
  if (deals?.status === "ok") {
    set(
      "multi_currency",
      deals.data.fields.some((f) => f.name === "CurrencyIsoCode") ? "present" : "absent",
      deals.data.fields.some((f) => f.name === "CurrencyIsoCode") ? "Opportunity has CurrencyIsoCode, which only multi-currency orgs have." : "Opportunity has no CurrencyIsoCode, which every multi-currency org has.",
    );
  }
  const described = schema ? Object.values(schema).filter((o) => o.status === "ok") : [];
  // A describe that lists none doesn't prove there are none, so that stays unknown.
  if (described.some((o) => o.status === "ok" && o.data.recordTypes.length > 0)) set("record_types", "present", "The describe of a core object lists record types.");
  return profile;
}
