import type { ManualStep } from "../schemas/index.ts";

type Template = Omit<ManualStep, "id" | "status" | "ruleIds">;

// Settings Salesforce doesn't expose through any API we can use. Each one says
// why it matters, where to look and what to bring back, so a consultant can
// answer without knowing the rule behind it.
export const MANUAL_STEPS: Record<string, Template> = {
  "eac-advanced-sync": {
    key: "eac-advanced-sync",
    domain: "utilisation",
    raisedBy: "engine",
    title: "Check Einstein Activity Capture advanced sync settings",
    whyItMatters:
      "These settings decide whether synced calendar events are related to Salesforce records. If they're off, events can sync while totals look normal and the deals they belong to show no activity. The settings can't be read by API.",
    whereToGo: "Setup > Einstein Activity Capture > Configurations > open each configuration > Advanced Sync Settings",
    whatToCapture: "Whether Relate Synced Events is on, and the days before connecting, for the configuration most sales users are on.",
    fields: [
      { key: "relate_synced_events", label: "Relate Synced Events is on", type: "boolean", required: true },
      { key: "days_before_connecting", label: "Days before connecting", type: "number", required: false },
    ],
    answerMethods: ["fields", "paste", "screenshot"],
    ifSkipped: "Activity capture configuration is reported as not measurable.",
  },
  "default-workflow-user": {
    key: "default-workflow-user",
    domain: "utilisation",
    raisedBy: "engine",
    title: "Confirm the Default Workflow User",
    whyItMatters:
      "If the Default Workflow User is deactivated, automation that runs as that user can fail. We couldn't confirm this setting is readable by API, so no user is recommended for licence removal until it's checked.",
    whereToGo: "Setup > Process Automation Settings > Default Workflow User",
    whatToCapture: "The username shown as the Default Workflow User.",
    fields: [{ key: "username", label: "Default Workflow User username", type: "text", required: true }],
    answerMethods: ["fields", "paste", "screenshot"],
    ifSkipped: "Every licence removal recommendation stays blocked and is shown for investigation only.",
  },
};
