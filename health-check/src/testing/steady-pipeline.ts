// Steady sales pipelines for cur.stuck-in-stage's tests (DECISIONS.md 777): the e44 and e45 probes' generator,
// trimmed. Deals arrive evenly for 1,095 days before AS_OF and move Prospecting, Discovery, Proposal, Negotiation;
// each stay is drawn from one of four shapes with a mean of 15 days, nothing changing over time; on leaving an open
// stage a deal is lost with chance 0.15, and after Negotiation it is won. Owners are the synthetic org's users 1, 2,
// 3, 14 and 15. A deal is read as the extract's scope reads it: open, or created or closed in the 24 months before.
// Made up, with zero-padded ids: nothing from a real org.

const DAY = 86_400_000;
const STAGES = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
const LOSS = 0.15;
const SPAN = 1095;
const OWNERS = [1, 2, 3, 14, 15].map((n) => `005${String(n).padStart(12, "0")}AAA`);

function seeded(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const normal = (r: () => number): number => Math.sqrt(-2 * Math.log(1 - r())) * Math.cos(2 * Math.PI * r());

/** The four stay shapes, each with a mean of 15 days a stage. */
export const STAY_SHAPES = {
  exponential: { draw: (r: () => number) => -15 * Math.log(1 - r()), mean: 15 },
  "lognormal s=1": { draw: (r: () => number) => (15 / Math.exp(0.5)) * Math.exp(normal(r)), mean: 15 },
  "lognormal s=1.5": { draw: (r: () => number) => (15 / Math.exp(1.125)) * Math.exp(1.5 * normal(r)), mean: 15 },
  "bimodal 80/20": { draw: (r: () => number) => (r() < 0.8 ? 2 + 4 * r() : 30 + 58 * r()), mean: 0.8 * 4 + 0.2 * 59 },
} as const;
export type StayShape = keyof typeof STAY_SHAPES;

export type PipelineProblem =
  | { kind: "none" }
  /** `share` x the open target extra deals sit in Proposal, entered 45 to 180 days ago, never moved. */
  | { kind: "abandoned"; share: number }
  /** A deal that will be lost from a stage sits there `factor` times as long first. */
  | { kind: "lostSlowly"; factor: number; noDecision?: boolean };

export type PipelineOptions = {
  /** Every stay gets a second history row in the same stage halfway through, as an Amount or close date edit makes. */
  edits?: boolean;
  /** Days of history before the run, 1,095 unless given: an org younger than the read. */
  historyDays?: number;
  /** Every stay this many times as long, the arrival rate scaled to keep the open target: a longer sales cycle. */
  stayScale?: number;
};

type Run = [stage: string, start: number, end: number | null];

/** The opportunities and stage history of a steady pipeline, as the opportunities extract holds them. */
export function steadyPipeline(shape: StayShape, openTarget: number, seed: number, now: Date, problem: PipelineProblem = { kind: "none" }, options: PipelineOptions = {}): { opportunities: unknown[]; history: unknown[] } {
  const r = seeded(seed);
  const dist = STAY_SHAPES[shape];
  const NOW = now.getTime();
  const windowFrom = NOW - 730 * DAY;
  const scale = options.stayScale ?? 1;
  const rate = openTarget / (scale * dist.mean * [0, 1, 2, 3].reduce((s, k) => s + (1 - LOSS) ** k, 0));
  const deals: Array<{ id: string; owner: string; created: number; runs: Run[]; closed: number | null; won: boolean }> = [];
  const add = (created: number, runs: Run[], closed: number | null, won: boolean) => deals.push({ id: `006${String(deals.length + 1).padStart(12, "0")}AAA`, owner: OWNERS[Math.floor(r() * OWNERS.length)]!, created, runs, closed, won });
  for (let t = NOW - (options.historyDays ?? SPAN) * DAY; ; ) {
    t += (-Math.log(1 - r()) / rate) * DAY;
    if (t >= NOW) break;
    const runs: Run[] = [];
    let at = t;
    let closed: number | null = null;
    let won = false;
    for (let s = 0; s < STAGES.length; s++) {
      const last = s === STAGES.length - 1;
      const lostHere = problem.kind === "lostSlowly" && !last ? r() < LOSS : false;
      let stay = Math.min(scale * dist.draw(r), 1000 * scale);
      if (lostHere && problem.kind === "lostSlowly") stay = Math.min(stay * problem.factor, 1000);
      const end = at + stay * DAY;
      if (end >= NOW) {
        runs.push([STAGES[s]!, at, null]);
        break;
      }
      runs.push([STAGES[s]!, at, end]);
      at = end;
      if (last) [closed, won] = [end, true];
      else if (problem.kind === "lostSlowly" ? lostHere : r() < LOSS) {
        closed = end;
        break;
      }
    }
    add(t, runs, closed, won);
  }
  if (problem.kind === "abandoned") {
    for (let i = 0; i < Math.round(problem.share * openTarget); i++) {
      const entered = NOW - (45 + 135 * r()) * DAY;
      const runs: Run[] = [];
      let at = entered - 2 * 5 * DAY;
      for (let s = 0; s < 2; s++, at += 5 * DAY) runs.push([STAGES[s]!, at, at + 5 * DAY]);
      runs.push(["Proposal", entered, null]);
      add(runs[0]![1], runs, null, false);
    }
  }
  const iso = (ms: number) => new Date(ms).toISOString().replace("Z", "+0000");
  const day = (ms: number) => new Date(ms).toISOString().slice(0, 10);
  const opportunities: unknown[] = [];
  const history: unknown[] = [];
  for (const d of deals) {
    if (!(d.closed === null || d.created >= windowFrom || d.closed >= windowFrom)) continue;
    const lostAs = problem.kind === "lostSlowly" && problem.noDecision ? "Closed No Decision" : "Closed Lost";
    const stage = d.closed === null ? d.runs.at(-1)![0] : d.won ? "Closed Won" : lostAs;
    const closeDate = d.closed === null ? day(NOW + 30 * DAY) : day(d.closed);
    opportunities.push({ Id: d.id, OwnerId: d.owner, StageName: stage, Amount: 10000, CloseDate: closeDate, CreatedDate: iso(d.created), IsClosed: d.closed !== null, IsWon: d.won, LastActivityDate: day(NOW - 3 * DAY), Type: "New Business", LeadSource: "Web", RecordType: "New_Business", CurrencyIsoCode: null, LossReason: null, AccountId: null });
    const row = (s: string, ms: number) => history.push({ Id: `008${String(history.length + 1).padStart(12, "0")}AAA`, OpportunityId: d.id, StageName: s, Amount: 10000, CloseDate: closeDate, CreatedDate: iso(ms) });
    for (const [s, start, end] of d.runs) {
      row(s, start);
      // An edit inside the stay, the same stage again: one stay, not two.
      if (options.edits) row(s, (start + (end ?? NOW)) / 2);
    }
    if (d.closed !== null) row(stage, d.closed);
  }
  return { opportunities, history };
}

/** The recorded extracts with their deals replaced by `pipeline`'s. */
export function withPipeline(extracts: Map<string, unknown>, pipeline: { opportunities: unknown[]; history: unknown[] }): Map<string, unknown> {
  const out = new Map(extracts);
  const opps = extracts.get("opportunities") as { opportunities: object; history: object };
  out.set("opportunities", { ...opps, opportunities: { ...opps.opportunities, data: pipeline.opportunities }, history: { ...opps.history, data: pipeline.history } });
  return out;
}
