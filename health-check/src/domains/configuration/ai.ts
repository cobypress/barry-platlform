import { aiSignalsExtractor, ARTICLE_CAP, SETTINGS, type SettingKey } from "../../extract/ai.ts";
import { apexExtractor, metadataExtractor } from "../../extract/automation.ts";
import { unreadReason } from "../../extract/metadata-parse.ts";
import { pct, type RuleContext } from "../../engine/rule.ts";
import type { Metric, MetricSegment } from "../../schemas/index.ts";

// The two figures AI readiness reads beyond the findings (DECISIONS.md 237 and
// 238): what's switched on, and what an agent could call. They describe; the
// verdicts are made from them, and the findings, in src/narrative/readiness.ts.

export const AI_PLATFORM = "ai.platform";
export const AI_ACTIONS = "ai.actions";
/** The share of active automation still in Workflow Rules and Process Builder, as a percent, with the working lines as segments. */
export const AI_LEGACY_SHARE = "ai.legacy_share";
export const AI_METRIC_IDS = [AI_PLATFORM, AI_ACTIONS, AI_LEGACY_SHARE];

// The working lines readiness judges against, our judgement (DECISIONS.md 240).
// Kept with the figures, so the raw notes state them and the number check can trace them.
/** Below this many published articles, Knowledge is "nearly": too few to ground most answers. */
export const ARTICLES_READY = 20;
/** The share of active automation in Workflow Rules and Process Builder that's still fine, and the share past which it's "not yet". */
export const LEGACY_READY = 10;
export const LEGACY_NOT_YET = 33;

export const SETTING_LABELS: Record<SettingKey, string> = { knowledge: "Salesforce Knowledge", generativeAi: "Einstein generative AI", agentforce: "Agentforce", dataCloud: "Data Cloud" };

type Ctx = Omit<RuleContext, "metrics">;
const DOMAIN = "configuration";

function platformMetric(ctx: Ctx): Metric {
  const { settings, agents, articles } = ctx.data(aiSignalsExtractor);
  const head = {
    id: AI_PLATFORM,
    domain: DOMAIN,
    label: "AI features switched on",
    definition: "Whether Salesforce Knowledge, Einstein generative AI, Agentforce and Data Cloud are switched on, as the org's settings say, with the agents set up and the Knowledge articles published.",
    unit: "count" as const,
    evidence: [ctx.ev(settings.source, { count: settings.status === "ok" ? Object.values(settings.data).filter((v) => v !== null).length : null }), ctx.ev(agents.source, { count: agents.status === "ok" ? agents.data.length : null }), ctx.ev(articles.source, { count: articles.status === "ok" ? articles.data.length : null })],
    exclusions: [],
    assumptions: ["A setting Salesforce didn't return is unknown, not off"],
  };
  if (settings.status !== "ok") return { ...head, status: "not_measurable", reason: `The org's settings couldn't be read: ${settings.reason}` };
  const keys = Object.keys(SETTINGS) as SettingKey[];
  const segments: MetricSegment[] = keys.map((k) => {
    const on = settings.data[k];
    return { dimension: "setting", key: SETTING_LABELS[k], n: 1, value: on === null ? null : on ? 1 : 0, notMeasurableReason: on === null ? "Salesforce didn't return this setting" : null };
  });
  segments.push(
    agents.status === "ok"
      ? { dimension: "agents", key: "Agents set up", n: agents.data.length, value: agents.data.length, notMeasurableReason: null }
      : { dimension: "agents", key: "Agents set up", n: 0, value: null, notMeasurableReason: agents.reason },
    articles.status === "ok"
      ? { dimension: "knowledge", key: "Published articles", n: articles.data.length, value: articles.data.length, notMeasurableReason: null }
      : { dimension: "knowledge", key: "Published articles", n: 0, value: null, notMeasurableReason: articles.reason },
  );
  const stats: Record<string, number> = Object.fromEntries(keys.flatMap((k) => (settings.data[k] === null ? [] : [[k, settings.data[k] ? 1 : 0]])));
  if (agents.status === "ok") stats.agents = agents.data.length;
  if (articles.status === "ok") Object.assign(stats, { articles: articles.data.length, articlesCapped: articles.truncated || articles.data.length >= ARTICLE_CAP ? 1 : 0, articlesReady: ARTICLES_READY });
  const known = keys.filter((k) => settings.data[k] !== null);
  return { ...head, status: "measured", value: known.filter((k) => settings.data[k]).length, n: known.length, dateRange: null, segments, segmentExemptReason: null, series: [], stats };
}

function actionsMetric(ctx: Ctx): Metric {
  const metadata = ctx.data(metadataExtractor);
  const { classes, triggers } = ctx.data(apexExtractor);
  const head = {
    id: AI_ACTIONS,
    domain: DOMAIN,
    label: "Automation an agent could call",
    definition: "Active autolaunched flows with no trigger and invocable Apex methods, which Salesforce lets an agent call as actions, and the share of active automation still in Workflow Rules and Process Builder, which it can't.",
    unit: "count" as const,
    evidence: [ctx.ev(metadata.source, { count: metadata.status === "ok" ? metadata.data.flows.length : null }), ctx.ev(classes.source, { count: classes.status === "ok" ? classes.data.length : null, note: "Read from the code: @InvocableMethod" })],
    exclusions: ["Inactive flows and code", "Test classes", "Installed packages' flows and code", "Record-triggered, scheduled and platform event flows, which run on their trigger"],
    assumptions: ["A flow or method is something an agent could call, not one it can use well: that also needs a clear description"],
  };
  if (metadata.status !== "ok") return { ...head, status: "not_measurable", reason: `Metadata couldn't be retrieved: ${metadata.reason}` };
  if (classes.status !== "ok") return { ...head, status: "not_measurable", reason: `Apex classes couldn't be read: ${classes.reason}` };
  const active = metadata.data.flows.filter((f) => f.status === "Active");
  // Record-triggered, scheduled and platform event flows are AutoLaunchedFlow too,
  // but they run on their trigger: only one with no trigger can be an agent's action.
  const autolaunched = active.filter((f) => f.processType === "AutoLaunchedFlow" && !f.triggerType && !f.scheduled);
  const described = autolaunched.filter((f) => f.described).length;
  const invocable = classes.data.filter((c) => c.Status === "Active" && !c.isTest).reduce((sum, c) => sum + (c.scan?.invocable ?? 0), 0);
  // The share needs every kind of automation read: a kind that couldn't be read
  // isn't none, and counting it as none would move the share (rule 5).
  const legacyUnread = legacyUnreadReason(ctx);
  const legacy = metadata.data.workflows.reduce((sum, w) => sum + w.rules.filter((r) => r.active).length, 0) + active.filter((f) => f.processType === "Workflow").length;
  const modern = active.filter((f) => f.processType !== "Workflow").length + (triggers.status === "ok" ? triggers.data.filter((t) => t.Status === "Active").length : 0);
  const segments: MetricSegment[] = [
    { dimension: "action", key: "Autolaunched flows", n: autolaunched.length, value: autolaunched.length, notMeasurableReason: null },
    { dimension: "action", key: "Invocable Apex methods", n: invocable, value: invocable, notMeasurableReason: null },
  ];
  return {
    ...head,
    status: "measured",
    value: autolaunched.length + invocable,
    n: autolaunched.length + invocable,
    dateRange: null,
    segments,
    segmentExemptReason: null,
    series: [],
    stats: { autolaunched: autolaunched.length, described, invocable, ...(legacyUnread ? {} : { legacy, automation: legacy + modern }) },
  };
}

/** Why the share of old automation can't be worked out, or null when it can. */
function legacyUnreadReason(ctx: Ctx): string | null {
  const metadata = ctx.data(metadataExtractor);
  const { triggers } = ctx.data(apexExtractor);
  if (metadata.status === "ok") {
    const unread = unreadReason(metadata.data, ["workflowRules"]);
    if (unread) return unread;
  }
  return triggers.status === "ok" ? null : `Apex triggers couldn't be read: ${triggers.reason}`;
}

// Its own metric, so the share and the lines read as percents in the number check.
function legacyMetric(ctx: Ctx, actions: Metric): Metric {
  const unread = legacyUnreadReason(ctx);
  const head = {
    id: AI_LEGACY_SHARE,
    domain: DOMAIN,
    label: "Automation still in old tools",
    definition: "Active Workflow Rules and Process Builders as a share of all active automation: those, flows of every kind and Apex triggers. Salesforce is retiring the old tools, and an agent can't call them.",
    unit: "percent" as const,
    evidence: actions.evidence,
    // Every kind of flow counts here, triggered or not: only inactive and packaged automation is left out.
    exclusions: ["Inactive flows and code", "Test classes", "Installed packages' flows and code"],
    assumptions: [`Up to ${LEGACY_READY}% is ready, and past ${LEGACY_NOT_YET}% is not yet: our working lines`],
  };
  if (actions.status !== "measured") return { ...head, status: "not_measurable", reason: actions.reason };
  if (unread) return { ...head, status: "not_measurable", reason: unread };
  const legacy = actions.stats.legacy!;
  const automation = actions.stats.automation!;
  return {
    ...head,
    status: "measured",
    value: pct(legacy, automation),
    n: automation,
    dateRange: null,
    segments: [
      { dimension: "working line", key: "Ready up to", n: 0, value: LEGACY_READY, notMeasurableReason: null },
      { dimension: "working line", key: "Not yet past", n: 0, value: LEGACY_NOT_YET, notMeasurableReason: null },
    ],
    segmentExemptReason: null,
    series: [],
    stats: { legacy, automation },
  };
}

export function aiMetrics(ctx: Ctx): Metric[] {
  const actions = actionsMetric(ctx);
  return [platformMetric(ctx), actions, legacyMetric(ctx, actions)];
}
