import type { Run } from "../schemas/index.ts";

// The worker's switch for automatic runs (DECISIONS.md 688), read once at
// start in env.ts. An automatic run is one no person reviews before the
// customer reads it, so until it is built and proven it never reads an org for
// analysis: with the switch off, or the org not listed, its connect is refused
// and its run fails at its claim. With both on, it connects, is calibrated from
// the org's settings and queued (692), and its run still fails just before
// extraction, because nothing past that is built yet (lane E3 takes that
// refusal away). A consultant-led run never looks here.

export type AutomaticSwitch = {
  /** HC_AUTOMATIC is exactly "on". */
  on: boolean;
  /** HC_AUTOMATIC_ORGS: the 15-character org ids automatic runs are allowed for during the proof. */
  orgs: ReadonlySet<string>;
};

export const AUTOMATIC_SWITCHED_OFF = "Automatic runs are switched off on this worker, so nothing was read from Salesforce.";
export const AUTOMATIC_ORG_NOT_LISTED = "Automatic runs aren't allowed for this org on this worker yet, so nothing was read from Salesforce.";
/** Said just before extraction, until lane E3 builds what comes after it (DECISIONS.md 692). */
export const AUTOMATIC_NOT_BUILT = "Automatic runs aren't built yet, so nothing was read from Salesforce.";

/** Why the switch won't let this run go on, or null: every consultant-led run, and an automatic one for a listed org with the switch on. */
export function automaticRefusal(run: Pick<Run, "reviewMode" | "orgId">, automatic: AutomaticSwitch | undefined): string | null {
  if (run.reviewMode === "consultant") return null;
  if (!automatic?.on) return AUTOMATIC_SWITCHED_OFF;
  if (!automatic.orgs.has(run.orgId.slice(0, 15))) return AUTOMATIC_ORG_NOT_LISTED;
  return null;
}

/** The one line the worker logs at start, so a deploy that changes the switch shows in the log. Counts, never the org ids. */
export function automaticStartLine(automatic: AutomaticSwitch): string {
  if (!automatic.on) return "Automatic runs are off: HC_AUTOMATIC isn't on, so an automatic run's connect is refused and its run fails at its claim.";
  const n = automatic.orgs.size;
  return `Automatic runs are switched on for ${n === 1 ? "1 org" : `${n} orgs`}: one connects, is calibrated and queued, but isn't built past that yet, so it fails before reading the org for analysis.`;
}

const ORG_ID = /^[A-Za-z0-9]{15}$/;

/** HC_AUTOMATIC_ORGS as a set, or the entries that aren't 15-character org ids. Blank entries are ignored. */
export function readAutomaticOrgs(raw: string): { ok: true; orgs: Set<string> } | { ok: false; bad: number } {
  const entries = raw.split(",").map((e) => e.trim()).filter(Boolean);
  const bad = entries.filter((e) => !ORG_ID.test(e)).length;
  return bad ? { ok: false, bad } : { ok: true, orgs: new Set(entries) };
}
