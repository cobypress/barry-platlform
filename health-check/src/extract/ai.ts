import { XMLParser } from "fast-xml-parser";
import { isUnavailable } from "../salesforce/client.ts";
import { readSoql, unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// What an AI readiness verdict needs that nothing else reads (roadmap Stage 6,
// DECISIONS.md 237): four settings from one Metadata API retrieve, the agents
// the org has, and how many Knowledge articles are published. Set-up only.
//
// Each setting is named in the Metadata API Developer Guide:
// KnowledgeSettings.enableKnowledge, EinsteinGptSettings.enableEinsteinGptPlatform,
// AgentPlatformSettings.enableAgentPlatform (Agentforce) and
// CustomerDataPlatformSettings.enableCustomerDataPlatform (Data Cloud, now Data 360).
// A setting that doesn't come back is unknown, never false.

export const SETTINGS = {
  knowledge: { type: "KnowledgeSettings", field: "enableKnowledge", member: "Knowledge" },
  generativeAi: { type: "EinsteinGptSettings", field: "enableEinsteinGptPlatform", member: "EinsteinGpt" },
  agentforce: { type: "AgentPlatformSettings", field: "enableAgentPlatform", member: "AgentPlatform" },
  dataCloud: { type: "CustomerDataPlatformSettings", field: "enableCustomerDataPlatform", member: "CustomerDataPlatform" },
} as const;
export type SettingKey = keyof typeof SETTINGS;
export type AiSettings = Record<SettingKey, boolean | null>;

/** The most published articles counted. More than this is "at least". */
export const ARTICLE_CAP = 2_000;

export type AiSignals = {
  settings: Sourced<AiSettings>;
  agents: Sourced<Array<{ Id: string; DeveloperName: string }>>;
  /** Published articles' ids, up to the cap. Unavailable when Knowledge is off or unknown. */
  articles: Sourced<Array<{ Id: string }>>;
};

const parser = new XMLParser({ removeNSPrefix: true, ignoreAttributes: true, parseTagValue: false });

/** Each setting from whichever file holds its type, whatever the file is called. */
export function parseSettings(files: Record<string, string>): AiSettings {
  const roots: Record<string, Record<string, unknown>> = {};
  for (const xml of Object.values(files)) {
    try {
      for (const [type, body] of Object.entries(parser.parse(xml) as Record<string, unknown>)) if (body && typeof body === "object") roots[type] = body as Record<string, unknown>;
    } catch {
      // A file we can't parse leaves its settings unknown.
    }
  }
  const read = (key: SettingKey): boolean | null => {
    const value = roots[SETTINGS[key].type]?.[SETTINGS[key].field];
    return value === "true" ? true : value === "false" ? false : null;
  };
  return { knowledge: read("knowledge"), generativeAi: read("generativeAi"), agentforce: read("agentforce"), dataCloud: read("dataCloud") };
}

export const SETTINGS_SOURCE: SourceInfo = { kind: "metadata", path: `Settings: ${Object.values(SETTINGS).map((s) => s.member).join(", ")}` };

export const aiSignalsExtractor: Extractor<AiSignals> = {
  id: "ai-signals",
  version: 1,
  async run(ctx) {
    let settings: Sourced<AiSettings>;
    try {
      const files = await ctx.client.retrieveMetadata([{ name: "Settings", members: Object.values(SETTINGS).map((s) => s.member) }]);
      settings = { status: "ok", data: parseSettings(files), source: SETTINGS_SOURCE, truncated: false };
    } catch (err) {
      // A read-only connection can't use the Metadata API (DECISIONS.md 73), and we know no other route to these settings.
      if (!isUnavailable(err)) throw err;
      settings = unavailable(SETTINGS_SOURCE, err);
    }
    // BotDefinition is "Einstein Bots or Agentforce Agents" (Object Reference, API 60.0), and readable only where agents are enabled.
    const agents = await readSoql<{ Id: string; DeveloperName: string }>(ctx, "SELECT Id, DeveloperName FROM BotDefinition");
    const articleSoql = "SELECT Id FROM KnowledgeArticleVersion WHERE PublishStatus = 'Online' AND IsLatestVersion = true";
    const articles: Sourced<Array<{ Id: string }>> =
      settings.status === "ok" && settings.data.knowledge === true
        ? await readSoql(ctx, articleSoql, { maxRecords: ARTICLE_CAP })
        : { status: "unavailable", reason: settings.status === "ok" && settings.data.knowledge === false ? "Salesforce Knowledge is switched off." : "Whether Salesforce Knowledge is on couldn't be read.", source: { kind: "soql", api: "rest", query: articleSoql } };
    return { settings, agents, articles };
  },
};
