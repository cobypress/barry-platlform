import { fieldDescribeExtractor } from "../../extract/fields.ts";
import { loginsExtractor, usersExtractor, type UserRow } from "../../extract/users.ts";
import { customObjectsExtractor, orgExtractor, recordCountsExtractor, type SharingDefaults } from "../../extract/configuration.ts";
import { orgDocumentationExtractor } from "../../extract/documentation.ts";
import { privilegedPermissionsExtractor, securityHealthExtractor, type HealthRiskRow } from "../../extract/security.ts";
import { notSized, plural, segmentRate, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import type { DomainModule } from "../types.ts";

// Working limits, stated in findings as our judgement rather than Salesforce's.
const HEALTH_SCORE_TARGET = 80;
const HEALTH_SCORE_POOR = 60;
const PRIVILEGED_SHARE_LIMIT = 10;
const PRIVILEGED_MIN_USERS = 3;
const MAX_LISTED_SETTINGS = 5;
const ADMIN_PROFILE = "System Administrator";
const EXPECTED_PRIVILEGED_KINDS = new Set(["admin", "integration", "system"]);
const OPEN_ACCESS = new Set(["Edit", "ReadEditTransfer"]);
const SHARING_FIELDS: Array<[string, keyof SharingDefaults]> = [["Account", "DefaultAccountAccess"], ["Contact", "DefaultContactAccess"], ["Opportunity", "DefaultOpportunityAccess"], ["Lead", "DefaultLeadAccess"]];

const riskGroups = (risks: HealthRiskRow[]): number => new Set(risks.map((r) => r.SettingGroup ?? "No group")).size;
const kindOf = (ctx: RuleContext, id: string): string | undefined => ctx.calibration.users.find((u) => u.userId.slice(0, 15) === id.slice(0, 15))?.kind;

const healthScore: Rule = {
  id: "sec.health-check-score",
  version: 1,
  domain: "security",
  title: "Salesforce Security Health Check score",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { score, risks } = ctx.data(securityHealthExtractor);
    if (score.status !== "ok") return { outcome: "not_measurable", reason: `The Health Check score couldn't be read: ${score.reason}`, evidence: [ctx.ev(score.source, { count: null })] };
    const raw = score.data[0]?.Score;
    const value = raw === null || raw === undefined || raw.trim() === "" ? NaN : Number(raw);
    if (!Number.isFinite(value)) return { outcome: "not_measurable", reason: "Salesforce returned no Health Check score for this org.", evidence: [ctx.ev(score.source, { count: score.data.length })] };
    const evidence = [ctx.ev(score.source, { count: 1, note: "Scored by Salesforce against its own baseline, unless the org has imported a custom one" })];
    if (value >= HEALTH_SCORE_TARGET) {
      // A good score can sit on a high risk the weighting hides, so it needs the settings behind it.
      if (risks.status !== "ok") return { outcome: "not_measurable", reason: `The score is ${value}, but the settings behind it couldn't be read: ${risks.reason}`, evidence };
      return { outcome: "pass", summary: `The Health Check score is ${value}, at or above our working target of ${HEALTH_SCORE_TARGET}.`, evidence, segmentsChecked: [{ dimension: "setting group", segments: riskGroups(risks.data) }], facts: { score: value } };
    }
    return {
      outcome: "issue",
      title: "Salesforce rates your security settings below target",
      summary: `Your Health Check score is ${value}. We use ${HEALTH_SCORE_TARGET} as a working target, which is our judgement rather than a Salesforce threshold.`,
      recommendation: "Work through the high-risk settings first, because they carry the most weight in the score.",
      recommendationType: "change",
      severity: value < HEALTH_SCORE_POOR ? "high" : "medium",
      confidence: "high",
      evidence,
      value: notSized("A security gap is a risk to manage, not a figure we can put in revenue or hours."),
      effort: { size: "M" },
      owner: "Salesforce admin",
      dependencies: ["Check each change with whoever owns single sign-on and integrations, because session and password settings can lock out logins"],
      segmentsChecked: risks.status === "ok" ? [{ dimension: "setting group", segments: riskGroups(risks.data) }] : [],
      facts: { score: value, target: HEALTH_SCORE_TARGET },
    };
  },
};

const healthRisks: Rule = {
  id: "sec.health-check-risks",
  version: 1,
  domain: "security",
  title: "Security settings below Salesforce's standard",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { risks } = ctx.data(securityHealthExtractor);
    if (risks.status !== "ok") return { outcome: "not_measurable", reason: `Health Check settings couldn't be read: ${risks.reason}`, evidence: [ctx.ev(risks.source, { count: null })] };
    if (risks.data.length === 0) return { outcome: "not_measurable", reason: "Salesforce returned no Health Check settings, so we can't say they meet the standard.", evidence: [ctx.ev(risks.source, { count: 0 })] };
    const high = risks.data.filter((r) => r.RiskType === "HIGH_RISK");
    const medium = risks.data.filter((r) => r.RiskType === "MEDIUM_RISK");
    const known = new Set(["HIGH_RISK", "MEDIUM_RISK", "MEETS_STANDARD"]);
    const other = risks.data.filter((r) => !known.has(r.RiskType)).length;
    const evidence = [ctx.ev(risks.source, { count: risks.data.length, exclusions: other ? [`${plural(other, "setting")} with a risk type other than high, medium or meets standard`] : [] })];
    const segments = [{ dimension: "setting group", segments: riskGroups(risks.data) }, { dimension: "risk type", segments: new Set(risks.data.map((r) => r.RiskType)).size }];
    if (high.length === 0 && medium.length === 0) return { outcome: "pass", summary: `None of the ${risks.data.length} settings Salesforce checks is rated high or medium risk.`, evidence, segmentsChecked: segments };
    const label = (r: HealthRiskRow): string => `${r.SettingGroup ?? "No group"}: ${r.Setting}${r.OrgValue || r.StandardValue ? ` (yours: ${r.OrgValue ?? "not set"}, standard: ${r.StandardValue ?? "not stated"})` : ""}`;
    return {
      outcome: "issue",
      title: high.length > 0 ? "High-risk security settings need changing" : "Some security settings are below Salesforce's standard",
      summary: `Of ${plural(risks.data.length, "setting")} Salesforce checks, ${high.length} ${high.length === 1 ? "is" : "are"} rated high risk and ${medium.length} medium risk.`,
      recommendation: "Bring the high-risk settings up to Salesforce's standard value, then plan the medium-risk ones.",
      recommendationType: "change",
      severity: high.length > 0 ? "high" : "low",
      confidence: "high",
      evidence,
      value: notSized("A security gap is a risk to manage, not a figure we can put in revenue or hours."),
      effort: { size: high.length > MAX_LISTED_SETTINGS ? "M" : "S" },
      owner: "Salesforce admin",
      dependencies: ["Check each change with whoever owns single sign-on and integrations"],
      segmentsChecked: segments,
      subjects: [...high, ...medium].slice(0, MAX_LISTED_SETTINGS).map((r) => ({ type: "setting", label: label(r) })),
      facts: { highRisk: high.length, mediumRisk: medium.length, settings: risks.data.length },
    };
  },
};

type Permission = "PermissionsModifyAllData" | "PermissionsViewAllData";

function privilegeRule(options: { id: string; title: string; permission: Permission; label: string; severity: "high" | "medium"; why: string }): Rule {
  return {
    id: options.id,
    version: 1,
    domain: "security",
    title: options.title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const { sets, assignments } = ctx.data(privilegedPermissionsExtractor);
      const users = ctx.data(usersExtractor);
      if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
      if (sets.status !== "ok") return { outcome: "not_measurable", reason: `Permission sets couldn't be read: ${sets.reason}`, evidence: [ctx.ev(sets.source, { count: null })] };
      if (assignments.status !== "ok") return { outcome: "not_measurable", reason: `Permission set assignments couldn't be read: ${assignments.reason}`, evidence: [ctx.ev(assignments.source, { count: null })] };

      const granting = sets.data.filter((s) => s[options.permission]);
      const profiles = new Set(granting.flatMap((s) => (s.IsOwnedByProfile && s.Profile ? [s.Profile.Name] : [])));
      const added = new Set(granting.filter((s) => !s.IsOwnedByProfile).map((s) => s.Id));
      const fromSet = new Set(assignments.data.filter((a) => added.has(a.PermissionSetId)).map((a) => a.AssigneeId));
      const fromProfile = (u: UserRow): boolean => profiles.has(u.Profile?.Name ?? "");
      const holds = (u: UserRow): boolean => fromProfile(u) || fromSet.has(u.Id);
      const expected = (u: UserRow): boolean => {
        const kind = kindOf(ctx, u.Id);
        if (kind) return EXPECTED_PRIVILEGED_KINDS.has(kind);
        return u.Profile?.Name === ADMIN_PROFILE;
      };

      const active = users.data.filter((u) => u.IsActive);
      const result = segmentRate(active, holds, { profile: (u) => u.Profile?.Name ?? "No profile", "how it's granted": (u) => (fromSet.has(u.Id) ? "permission set" : fromProfile(u) ? "profile" : "not held") }, { segmentLimit: 100, minSegment: 1 });
      const holders = active.filter(holds);
      const unexpected = holders.filter((u) => !expected(u));
      const tooMany = holders.length >= PRIVILEGED_MIN_USERS && result.rate > PRIVILEGED_SHARE_LIMIT;
      const evidence = [ctx.ev(sets.source, { count: granting.length }), ctx.ev(assignments.source, { count: assignments.data.length, exclusions: ["Inactive users", "Non-standard user types"] })];
      if (!tooMany && unexpected.length === 0) {
        return { outcome: "pass", summary: `${options.label}: ${holders.length} of ${active.length} active users, all of them admins or calibrated system accounts.`, evidence, segmentsChecked: result.segmentsChecked, facts: { holders: holders.length, active: active.length, unexpected: 0 } };
      }
      return {
        outcome: "issue",
        title: unexpected.length > 0 ? `${options.label} is held by people outside your admin team` : `Too many people hold ${options.label}`,
        summary: `${options.label}: ${holders.length} of ${active.length} active users (${result.rate}%), ${unexpected.length} of them outside the admin profile and the accounts calibrated as admin or system. ${options.why}`,
        recommendation: `Remove ${options.label} from anyone who doesn't need it, and give them the specific object access they do need.`,
        recommendationType: "change",
        severity: options.severity,
        confidence: "high",
        evidence,
        value: notSized("A security gap is a risk to manage, not a figure we can put in revenue or hours."),
        effort: { size: "S" },
        owner: "Salesforce admin",
        dependencies: ["Check what each person uses this access for before removing it"],
        segmentsChecked: result.segmentsChecked,
        subjects: (unexpected.length > 0 ? unexpected : holders).map((u) => ({ type: "user", label: ctx.userLabel(u.Id) })),
        facts: { holders: holders.length, active: active.length, unexpected: unexpected.length, rate: result.rate },
      };
    },
  };
}

const modifyAllData = privilegeRule({
  id: "sec.modify-all-data",
  title: "Who can change every record",
  permission: "PermissionsModifyAllData",
  label: "Modify All Data",
  severity: "high",
  why: "This overrides sharing, so one compromised or careless login can change or delete anything.",
});

const viewAllData = privilegeRule({
  id: "sec.view-all-data",
  title: "Who can read every record",
  permission: "PermissionsViewAllData",
  label: "View All Data",
  severity: "medium",
  why: "This overrides sharing, so these people can read and export every record.",
});

const sharingDefaults: Rule = {
  id: "sec.sharing-defaults",
  version: 1,
  domain: "security",
  title: "Organisation-wide sharing defaults",
  evaluate(ctx): RuleResult | RuleResult[] {
    const sharing = ctx.data(orgExtractor);
    if (sharing.status !== "ok") return { outcome: "not_measurable", reason: `Sharing defaults couldn't be read: ${sharing.reason}`, evidence: [ctx.ev(sharing.source, { count: null })] };
    const row = sharing.data[0];
    const read = SHARING_FIELDS.map(([object, field]) => ({ object, access: row?.[field] ?? null })).filter((r) => r.access !== null);
    if (read.length === 0) return { outcome: "not_measurable", reason: "Salesforce returned no sharing defaults.", evidence: [ctx.ev(sharing.source, { count: 0 })] };
    const open = read.filter((r) => OPEN_ACCESS.has(r.access!));
    const evidence = [ctx.ev(sharing.source, { count: read.length, exclusions: ["Custom objects", "Sharing rules, which can only widen these defaults"] })];
    const segments = [{ dimension: "object", segments: read.length }];
    if (open.length === 0) return { outcome: "pass", summary: `None of the ${read.length} core objects is public read/write by default.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Everyone can edit some core records by default",
      summary: `${open.map((r) => r.object).join(", ")} ${open.length === 1 ? "is" : "are"} public read/write, so every internal user can change those records. Salesforce's guidance is to set defaults at the most restrictive level the business can work with, then open access up with roles and sharing rules.`,
      recommendation: "Decide with the business who needs to edit these records. Small teams may choose this on purpose, and if so, record that decision.",
      recommendationType: "investigate",
      severity: "medium",
      // Open sharing can be a deliberate choice, and only the customer can say.
      confidence: "medium",
      evidence,
      value: notSized("A security gap is a risk to manage, not a figure we can put in revenue or hours."),
      effort: { size: "M", note: "Tightening a default recalculates sharing and can change what people see" },
      owner: "Salesforce admin",
      dependencies: ["Test any change in a sandbox, because reports and automation can depend on this access"],
      segmentsChecked: segments,
      subjects: open.map((r) => ({ type: "object", label: r.object })),
      facts: { openObjects: open.length, objects: read.length },
    };
  },
};

export const securityModule: DomainModule = {
  id: "security",
  cloud: "sales",
  label: "Security and access",
  extractors: [usersExtractor, fieldDescribeExtractor, loginsExtractor, securityHealthExtractor, privilegedPermissionsExtractor, recordCountsExtractor, customObjectsExtractor, orgDocumentationExtractor],
  rules: [healthScore, healthRisks, modifyAllData, viewAllData, sharingDefaults],
};
