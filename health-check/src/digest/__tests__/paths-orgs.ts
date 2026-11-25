import { cpSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { readCheckpoint, writeCheckpoint } from "../../engine/checkpoint.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { HistoryRow, OpportunityExtract, OpportunityRow } from "../../extract/opportunities.ts";
import { loadRun } from "../../render/render-run.ts";
import { refusalSentence } from "../../salesforce/refusal-words.ts";
import type { Calibration } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// Orgs built to reach every branch of the digest's paths block (DECISIONS.md
// 758) through the real analysis: the synthetic org's automatic run, its
// opportunities and their stage history replaced by deals made by hand, then
// analysed again by resuming the run, so every finding, mark and pseudonym is
// the engine's own. No insight is injected. No real data.

export const calibration = await automaticSyntheticCalibration();
/** One automatic run per calibration, which a resumed run must keep. */
const bases = new Map<string, Promise<{ dir: string }>>();
const baseFor = (c: Calibration): Promise<{ dir: string }> => {
  const key = JSON.stringify(c);
  if (!bases.has(key)) bases.set(key, runFromRecording({ calibration: c, reviewMode: "automatic", transport: new AutomaticReplay() }));
  return bases.get(key)!;
};
const DAY = 86_400_000;

type Outcome = "won" | "lost";
/** `segment` is the deal's type, lead source and record type alike. `open`: still open in its last stage. */
export type Spec = { path: string[]; outcome: Outcome; days: number; segment?: string; open?: boolean; createdAt?: number; gapDays?: number };
const CLOSED: Record<Outcome, string> = { won: "Closed Won", lost: "Closed Lost" };
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");

/** `count` deals along `path`, `won` of them won and the rest lost, each `days` from creation to close (`wonDays` for the won). */
export const deals = (count: number, path: string[], won: number, days: number, segment?: string, wonDays = days): Spec[] =>
  Array.from({ length: count }, (_, i) => ({ path, outcome: i < won ? "won" : "lost", days: i < won ? wonDays : days, ...(segment ? { segment } : {}) }));
export const open = (n: number, path: string[]): Spec[] => deals(n, path, 0, 60).map((d) => ({ ...d, open: true }));

export const USUAL = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
export const SKIPS = ["Prospecting", "Proposal", "Negotiation"];
export const REWORK = ["Prospecting", "Discovery", "Prospecting", "Discovery", "Proposal", "Negotiation"];

export type Org = { stages?: Record<string, string>; history?: "ok" | "none" | "unavailable"; extraStages?: string[] };

let n = 0;
/** An automatic run of the synthetic org with these deals in place of its own, analysed through the real engine. `stages` renames a stage everywhere. */
export async function orgWith(specs: Spec[], org: Org = {}): Promise<ReturnType<typeof loadRun> & { dir: string }> {
  const rename = (s: string): string => org.stages?.[s] ?? s;
  const outDir = tempDir();
  const id = `hc-paths-${++n}`;
  const dir = join(outDir, id);
  const named: Calibration = {
    ...calibration,
    stages: [
      ...calibration.stages.map((s) => ({ ...s, name: rename(s.name) })),
      ...(org.extraStages ?? []).map((name, i) => ({ name, category: "open" as const, order: 100 + i })),
    ],
  };
  cpSync((await baseFor(named)).dir, dir, { recursive: true });
  const state = JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as Record<string, unknown>;
  writeFileSync(join(dir, "run.json"), JSON.stringify({ ...state, id, status: "stopped_budget" }));
  const at = join(dir, "checkpoints", "opportunities.json");
  const e = readCheckpoint<OpportunityExtract>(at);
  const owners = e.opportunities.status === "ok" ? [...new Set(e.opportunities.data.map((o) => o.OwnerId))] : ["005000000000000001"];
  const opportunities: OpportunityRow[] = [];
  const rows: HistoryRow[] = [];
  specs.forEach((s, i) => {
    const oid = `006${String(900_000 + i).padStart(15, "0")}`;
    const created = s.createdAt ?? Date.parse("2025-04-01T09:00:00Z") + i * 3_600_000;
    const closed = created + s.days * DAY;
    const closeDate = new Date(closed).toISOString().slice(0, 10);
    const path = s.path.map(rename);
    const final = s.open ? path.at(-1)! : CLOSED[s.outcome];
    opportunities.push({ Id: oid, OwnerId: owners[i % owners.length]!, StageName: final, Amount: null, CloseDate: closeDate, CreatedDate: stamp(created), IsClosed: !s.open, IsWon: !s.open && s.outcome === "won", LastActivityDate: null, Type: s.segment ?? null, LeadSource: s.segment ?? null, RecordType: s.segment ?? null, CurrencyIsoCode: null, LossReason: null });
    path.forEach((stage, k) => rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: oid, StageName: stage, Amount: null, CloseDate: closeDate, CreatedDate: stamp(created + k * (s.gapDays ? s.gapDays * DAY : 60_000)) }));
    if (!s.open) rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: oid, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(closed) });
  });
  if (e.opportunities.status === "ok") e.opportunities = { ...e.opportunities, data: opportunities };
  if (e.history.status === "ok") {
    const history = org.history ?? "ok";
    if (history === "ok") e.history = { ...e.history, data: rows };
    if (history === "none") e.history = { ...e.history, data: [] };
    if (history === "unavailable") e.history = { status: "unavailable", reason: refusalSentence("OpportunityFieldHistory", "no_permission"), source: e.history.source } as OpportunityExtract["history"];
  }
  writeCheckpoint(at, e);
  await runHealthCheck({ provider: syntheticProvider, calibration: named, modules: SALES_CLOUD_MODULES, outDir, resume: id, now: new Date(AS_OF), transport: new AutomaticReplay(), connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic" });
  return { ...loadRun(dir), dir };
}
