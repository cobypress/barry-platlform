import { AI_ACTIONS, AI_LEGACY_SHARE, AI_PLATFORM, ARTICLES_READY, LEGACY_NOT_YET, LEGACY_READY } from "../domains/configuration/ai.ts";
import type { Finding, Metric, Run } from "../schemas/index.ts";
import { isSetupReview, issuesOf, SEVERITY_ORDER } from "./select.ts";

// Readiness for Data Cloud and Agentforce (roadmap Stage 6, DECISIONS.md 236 to
// 243): could this org put an AI agent to work safely, and what would stop it?
// Code decides every verdict, from findings and the two AI metrics; Claude only
// words the section that introduces them (rule 1). There's no money figure:
// an "AI opportunity" would be invented (rule 5).

export type ReadinessVerdict = "ready" | "nearly" | "not_yet" | "not_measurable";
export const VERDICT_WORDS: Record<ReadinessVerdict, string> = { ready: "Ready", nearly: "Nearly", not_yet: "Not yet", not_measurable: "Not measurable" };

export type Pillar = { key: string; label: string; question: string; verdict: ReadinessVerdict; reason: string; findingIds: string[]; metricIds: string[] };
export type Readiness = {
  pillars: Pillar[];
  /** Said only when it's clear (DECISIONS.md 241): otherwise null, and the summaries leave it out. */
  overall: "ready" | "not_yet" | null;
  /** The issues to fix before an agent goes live, in order. At most five. */
  fixes: Finding[];
};

export const FIXES = 5;

/** Which findings feed which pillar. Data and access re-read whole areas; the others, named checks. */
const DATA_DOMAINS = ["data_quality"];
const ACCESS_DOMAINS = ["security"];
const ACCESS_RULES = ["int.integration-users-interactive", "sec.personal-integration-accounts", "sec.self-authorised-connected-apps"];
const ACTION_RULES = ["aut.undocumented-automation", "aut.flows-without-fault-paths"];
const SHAPE_RULES = ["aut.legacy-workflow-rules", "aut.legacy-process-builder", "aut.mixed-automation-on-object", "tech.governor-limit-risk"];
const KNOWLEDGE_RULES = ["svc.knowledge-current"];
/** When two fixes are as severe, the one an agent would hit first comes first. */
const FIX_ORDER = ["access", "data", "shape", "actions", "knowledge"];

const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;
const severityRank = (f: Finding): number => SEVERITY_ORDER.indexOf(f.severity);

// Words that keep their capital mid-sentence when a finding's title follows a colon.
const PROPER = new Set(["Salesforce", "Apex", "Modify", "View", "Einstein", "Agentforce", "Classic", "Lightning", "Knowledge", "Workflow", "Process", "Visualforce", "Account", "Service", "CPQ"]);

/** A finding's title as it reads after a colon: lower case, unless it opens with a name. */
export function inSentence(title: string): string {
  const first = title.split(/[\s,:;]/)[0] ?? "";
  if (PROPER.has(first) || (first.length > 1 && first === first.toUpperCase())) return title;
  return `${title.charAt(0).toLowerCase()}${title.slice(1)}`;
}

/** A verdict from findings alone: high or critical is "not yet", medium is "nearly", and nothing worse than low is "ready". */
function fromFindings(own: Finding[], noun: string): { verdict: ReadinessVerdict; reason: string } {
  const measured = own.filter((f) => f.status !== "not_measurable");
  if (measured.length === 0) return { verdict: "not_measurable", reason: `None of the checks on ${noun} could be measured.` };
  const issues = issuesOf(own);
  const serious = issues.filter((f) => f.severity === "critical" || f.severity === "high");
  const medium = issues.filter((f) => f.severity === "medium");
  const subject = `${noun.charAt(0).toUpperCase()}${noun.slice(1)}`;
  // No counts: the number check traces figures, and a count of issues by severity isn't one the dataset holds.
  if (serious.length) return { verdict: "not_yet", reason: `${subject} has ${serious.length === 1 ? "a critical or high issue" : "critical or high issues, such as"}: ${inSentence(serious[0]!.title)}.` };
  if (medium.length) return { verdict: "nearly", reason: `${subject} has nothing critical or high, but ${medium.length === 1 ? "a medium issue" : "medium issues, such as"}: ${inSentence(medium[0]!.title)}.` };
  return { verdict: "ready", reason: `${subject} has nothing above low severity.` };
}

const stat = (m: Metric | undefined, key: string): number | null => (m?.status === "measured" && key in m.stats ? m.stats[key]! : null);

export function readinessOf(run: Pick<Run, "scope">, findings: Finding[], metrics: Metric[]): Readiness {
  const platform = metrics.find((m) => m.id === AI_PLATFORM);
  const actions = metrics.find((m) => m.id === AI_ACTIONS);
  const ids = (fs: Finding[]): string[] => fs.map((f) => f.id);
  const byRule = (rules: string[]): Finding[] => findings.filter((f) => rules.includes(f.ruleId));
  const pillars: Pillar[] = [];

  // Data an agent reads and writes.
  const data = findings.filter((f) => DATA_DOMAINS.includes(f.domain));
  pillars.push({
    key: "data",
    label: "Data",
    question: "Can an agent trust what it reads?",
    ...(isSetupReview(run) && data.length === 0 ? { verdict: "not_measurable" as const, reason: "A set-up review doesn't read records, so the data itself is part of the full check." } : fromFindings(data, "data quality")),
    findingIds: ids(issuesOf(data)),
    metricIds: [],
  });

  // Knowledge an agent grounds its answers in.
  const knowledgeOn = stat(platform, "knowledge");
  const articles = stat(platform, "articles");
  const stale = byRule(KNOWLEDGE_RULES).filter((f) => f.status === "issue");
  const knowledge = ((): { verdict: ReadinessVerdict; reason: string } => {
    if (knowledgeOn === null) return { verdict: "not_measurable", reason: platform?.status === "not_measurable" ? platform.reason : "Whether Salesforce Knowledge is on couldn't be read." };
    if (knowledgeOn === 0) return { verdict: "not_yet", reason: "Salesforce Knowledge isn't switched on, so an agent has no articles to ground its answers in." };
    if (articles === null) return { verdict: "not_measurable", reason: "Salesforce Knowledge is on, but its published articles couldn't be counted." };
    if (articles === 0) return { verdict: "not_yet", reason: "Salesforce Knowledge is on, with no published articles." };
    const capped = stat(platform, "articlesCapped") === 1;
    const count = `${capped ? "at least " : ""}${plural(articles, "published article")}`;
    if (articles < ARTICLES_READY) return { verdict: "nearly", reason: `Salesforce Knowledge is on, with ${count}: fewer than ${ARTICLES_READY}, our working line for enough to ground most answers.` };
    if (stale.length) return { verdict: "nearly", reason: `Salesforce Knowledge is on, with ${count}, but ${stale[0]!.title.charAt(0).toLowerCase()}${stale[0]!.title.slice(1)}.` };
    return { verdict: "ready", reason: `Salesforce Knowledge is on, with ${count}.` };
  })();
  pillars.push({ key: "knowledge", label: "Knowledge", question: "Is there knowledge to ground answers in?", ...knowledge, findingIds: ids(stale), metricIds: [AI_PLATFORM] });

  // What's switched on.
  const genAi = stat(platform, "generativeAi");
  const agentforce = stat(platform, "agentforce");
  const dataCloud = stat(platform, "dataCloud");
  const agents = stat(platform, "agents");
  const extras = [dataCloud === null ? "" : `Data Cloud is ${dataCloud ? "on" : "off"}`, agents === null ? "" : `${plural(agents, "agent")} set up`].filter(Boolean).join(", and ");
  const switched = ((): { verdict: ReadinessVerdict; reason: string } => {
    if (genAi === null) return { verdict: "not_measurable", reason: platform?.status === "not_measurable" ? platform.reason : "Whether Einstein generative AI is on couldn't be read." };
    const tail = extras ? ` ${extras.charAt(0).toUpperCase()}${extras.slice(1)}.` : "";
    if (genAi === 0) return { verdict: "not_yet", reason: `Einstein generative AI isn't switched on, and Agentforce needs it.${tail}` };
    if (agentforce === 1) return { verdict: "ready", reason: `Einstein generative AI and Agentforce are both switched on.${tail}` };
    return { verdict: "nearly", reason: `Einstein generative AI is on, and Agentforce ${agentforce === 0 ? "isn't" : "couldn't be read"}.${tail}` };
  })();
  pillars.push({ key: "platform", label: "Platform", question: "Are Einstein, Data Cloud and Agentforce switched on?", ...switched, findingIds: [], metricIds: [AI_PLATFORM] });

  // Actions an agent could call.
  const actionFindings = byRule(ACTION_RULES);
  const undocumented = actionFindings.filter((f) => f.status === "issue");
  const callable = actions?.status === "measured" ? actions.value : null;
  const auto = stat(actions, "autolaunched");
  const described = stat(actions, "described");
  const invocable = stat(actions, "invocable");
  const act = ((): { verdict: ReadinessVerdict; reason: string } => {
    if (callable === null || auto === null || described === null || invocable === null) return { verdict: "not_measurable", reason: actions?.status === "not_measurable" ? actions.reason : "The automation an agent could call couldn't be read." };
    if (callable === 0) return { verdict: "not_yet", reason: "There's no active autolaunched flow or invocable Apex method for an agent to call." };
    const what = `${plural(auto, "active autolaunched flow")} (${described} described) and ${plural(invocable, "invocable Apex method")}`;
    if ((auto > 0 && described * 2 < auto) || undocumented.length) return { verdict: "nearly", reason: `${what}. An agent chooses an action by its description, and too few have one.` };
    return { verdict: "ready", reason: `${what}.` };
  })();
  pillars.push({ key: "actions", label: "Actions", question: "Is there automation an agent could call?", ...act, findingIds: ids(undocumented), metricIds: [AI_ACTIONS] });

  // Access an agent would inherit.
  const access = findings.filter((f) => ACCESS_DOMAINS.includes(f.domain) || ACCESS_RULES.includes(f.ruleId));
  pillars.push({ key: "access", label: "Access", question: "Would an agent inherit safe access?", ...fromFindings(access, "security and integration access"), findingIds: ids(issuesOf(access)), metricIds: [] });

  // The shape of the org's logic.
  const shapeFindings = byRule(SHAPE_RULES).filter((f) => f.status === "issue");
  const legacyMetric = metrics.find((m) => m.id === AI_LEGACY_SHARE);
  const share = legacyMetric?.status === "measured" ? legacyMetric.value : null;
  const legacy = stat(legacyMetric, "legacy");
  const shape = ((): { verdict: ReadinessVerdict; reason: string } => {
    if (share === null || legacy === null) return { verdict: "not_measurable", reason: legacyMetric?.status === "not_measurable" ? legacyMetric.reason : "How the org's automation is built couldn't be read." };
    const text = `${share}% of active automation is still in Workflow Rules and Process Builder, which an agent can't call and Salesforce is retiring`;
    if (share > LEGACY_NOT_YET) return { verdict: "not_yet", reason: `${text}.` };
    if (share > LEGACY_READY || shapeFindings.some((f) => f.severity === "critical" || f.severity === "high")) return { verdict: "nearly", reason: `${text}. Our working line is ${LEGACY_READY}%.` };
    return { verdict: "ready", reason: share === 0 ? "No active automation is left in Workflow Rules or Process Builder." : `${text}, within our working line of ${LEGACY_READY}%.` };
  })();
  pillars.push({ key: "shape", label: "Org shape", question: "Is the logic in tools an agent can work with?", ...shape, findingIds: ids(shapeFindings), metricIds: [AI_LEGACY_SHARE] });

  // Clear only with enough measured: two pillars not yet, or nothing worse than one "nearly".
  const measured = pillars.filter((p) => p.verdict !== "not_measurable");
  const notYet = measured.filter((p) => p.verdict === "not_yet").length;
  const nearly = measured.filter((p) => p.verdict === "nearly").length;
  const overall = measured.length < 4 ? null : notYet >= 2 ? "not_yet" : notYet === 0 && nearly <= 1 ? "ready" : null;

  const linked = new Map<string, string>();
  for (const p of pillars) for (const id of p.findingIds) if (!linked.has(id)) linked.set(id, p.key);
  const fixes = issuesOf(findings.filter((f) => linked.has(f.id)))
    .sort((a, b) => severityRank(a) - severityRank(b) || FIX_ORDER.indexOf(linked.get(a.id)!) - FIX_ORDER.indexOf(linked.get(b.id)!) || a.title.localeCompare(b.title))
    .slice(0, FIXES);
  return { pillars, overall, fixes };
}

/** The one line the executive summary and one-pager may carry, and only when the verdict is clear. No figures. */
export function readinessLine(r: Readiness): string | null {
  if (r.overall === null) return null;
  if (r.overall === "ready") return "Ready for an AI agent? Largely yes: the foundations are in place, and the full report sets out what's left.";
  const behind = r.pillars.filter((p) => p.verdict === "not_yet").map((p) => p.label.toLowerCase());
  const list = behind.length === 1 ? behind[0]! : `${behind.slice(0, -1).join(", ")} and ${behind.at(-1)}`;
  return `Ready for an AI agent? Not yet: ${list} ${behind.length === 1 ? "needs" : "need"} work first. The full report sets out what to fix.`;
}
