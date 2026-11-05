import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { strFromU8, unzipSync } from "fflate";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { channelOf, type LoginActivity } from "../../extract/adoption.ts";
import { usersExtractor, type UserRow } from "../../extract/users.ts";
import { buildReport } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { noteSections } from "../../render/references.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, WINDOW } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { AS_OF, userId } from "../../testing/synthetic-org.ts";
import { ADOPTION_METRIC_IDS, coveredMonths } from "../adoption/index.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const LOGINS = "login-activity";
type Sourced<T> = { status: string; data: T; missingFields?: string[] };
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const metric = (metrics: Metric[], id: string): Metric => {
  const found = metrics.find((m) => m.id === id);
  assert.ok(found, id);
  return found;
};
const valueOf = (m: Metric): number => {
  assert.equal(m.status, "measured", m.status === "not_measurable" ? `${m.id}: ${m.reason}` : m.id);
  return (m as Extract<Metric, { status: "measured" }>).value;
};
const change = <T>(id: string, edit: (data: T) => void, from = extracts): Map<string, unknown> => {
  const changed = clone(from);
  edit(changed.get(id) as T);
  return changed;
};

/** Every person logs in every day of the window: steady, busy use. */
const everyoneEveryDay = (): Map<string, unknown> => {
  const users = (extracts.get(usersExtractor.id) as Sourced<UserRow[]>).data;
  const days: string[] = [];
  for (let d = new Date("2025-11-01T00:00:00Z"); d < new Date(AS_OF); d = new Date(d.getTime() + 86_400_000)) days.push(d.toISOString().slice(0, 10));
  return change<Sourced<LoginActivity>>(LOGINS, (l) => {
    l.data.days = Object.fromEntries(users.map((u) => [u.Id, days]));
    l.data.earliest = "2025-11-01T09:00:00.000Z";
    l.data.from = "2025-11-01";
  });
};

describe("adoption traps fire on the synthetic org", () => {
  const { findings, metrics } = output!;

  test("falling adoption, org-wide and in the Standard User profile", () => {
    const f = one(findings, "adp.falling-adoption");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "high");
    assert.equal(f.facts.peak, 8);
    assert.equal(f.facts.latest, 5);
    assert.match(f.summary, /the whole org, from 8 in 2026-02 to 5 in 2026-09/);
    assert.match(f.summary, /profile Standard User/);
    assert.equal(f.value.kind, "not_sized");
    assert.ok(f.segmentsChecked.length > 0);
  });

  test("a profile and a role where most people barely log in", () => {
    const f = one(findings, "adp.low-engagement-group");
    assert.equal(f.status, "issue");
    assert.match(f.summary, /profile Standard User, 7 of 11/);
    assert.match(f.summary, /role Sales UK, 4 of 6/);
    assert.equal(f.value.kind, "not_sized");
  });

  test("the two Classic users, by label and never by name", () => {
    const f = one(findings, "adp.classic-users");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.users, 2);
    assert.equal(f.severity, "low");
    assert.equal(f.recommendationType, "change");
    assert.doesNotMatch(JSON.stringify(f), /Tom Baker|Ren Ito/);
    assert.equal(f.subjects.length, 2);
  });

  test("every adoption metric is measured, and the mobile app shows up", () => {
    for (const id of ADOPTION_METRIC_IDS) valueOf(metric(metrics, id));
    assert.ok(valueOf(metric(metrics, "adoption.mobile_login_share")) > 0);
    assert.equal(valueOf(metric(metrics, "adoption.lightning_share")), 83.3);
    const mau = metric(metrics, "adoption.monthly_active_users") as Extract<Metric, { status: "measured" }>;
    assert.deepEqual(mau.series.map((p) => p.value), [8, 8, 7, 6, 5, 5, 5, 5]);
  });

  test("a group of fewer than 5 has no figure of its own", () => {
    const m = metric(metrics, "adoption.daily_active_users") as Extract<Metric, { status: "measured" }>;
    const admin = m.segments.find((s) => s.key === "System Administrator")!;
    assert.equal(admin.value, null);
    assert.equal(admin.notMeasurableReason, "Fewer than 5 people");
  });
});

describe("adoption stays quiet on clean data", () => {
  test("steady logins: no falling adoption and no team that barely logs in", () => {
    const { findings } = analyseExtracts(everyoneEveryDay());
    assert.equal(one(findings, "adp.falling-adoption").status, "passed");
    assert.equal(one(findings, "adp.low-engagement-group").status, "passed");
  });

  test("everyone on Lightning", () => {
    const changed = change<Sourced<UserRow[]>>(usersExtractor.id, (u) => u.data.forEach((row) => (row.UserPreferencesLightningExperiencePreferred = true)));
    assert.equal(one(analyseExtracts(changed).findings, "adp.classic-users").status, "passed");
  });

  test("inactive Classic users don't count", () => {
    const changed = change<Sourced<UserRow[]>>(usersExtractor.id, (u) => u.data.filter((row) => row.UserPreferencesLightningExperiencePreferred === false).forEach((row) => (row.IsActive = false)));
    assert.equal(one(analyseExtracts(changed).findings, "adp.classic-users").status, "passed");
  });
});

describe("adoption is not measurable when the data can't support it", () => {
  test("login history holding one person's logins is not the whole org's (QUESTIONS.md 55)", () => {
    const changed = change<Sourced<LoginActivity>>(LOGINS, (l) => (l.data.days = { [userId(1)]: l.data.days[userId(1)]! }));
    const { findings, metrics } = analyseExtracts(changed);
    for (const id of ["adp.falling-adoption", "adp.low-engagement-group"]) {
      const f = one(findings, id);
      assert.equal(f.status, "not_measurable", id);
      assert.match(f.summary, /logins for 1 of the \d+ people .*can probably see only its own logins.*Monitor Login History/);
    }
    for (const id of ["adoption.daily_active_users", "adoption.weekly_active_users", "adoption.monthly_active_users", "adoption.monthly_active_share", "adoption.mobile_login_share"]) {
      const m = metric(metrics, id);
      assert.equal(m.status, "not_measurable", id);
      assert.match(m.status === "not_measurable" ? m.reason : "", /Monitor Login History/);
    }
    // The Lightning setting and reports don't come from the login history.
    valueOf(metric(metrics, "adoption.lightning_share"));
  });

  test("history covering half the people who logged in is enough", () => {
    const users = (extracts.get(usersExtractor.id) as Sourced<UserRow[]>).data;
    const from = new Date(WINDOW.from);
    const loggedIn = users.filter((u) => u.IsActive && u.LastLoginDate && new Date(u.LastLoginDate.replace(/\+0000$/, "Z")) >= from);
    const days = (extracts.get(LOGINS) as Sourced<LoginActivity>).data.days;
    assert.ok(loggedIn.filter((u) => days[u.Id]).length * 2 >= loggedIn.length);
    assert.equal(one(output!.findings, "adp.falling-adoption").status, "issue");
  });

  test("a Lightning setting empty for everyone is not measurable, never 0% (QUESTIONS.md 56)", () => {
    const changed = change<Sourced<UserRow[]>>(usersExtractor.id, (u) => u.data.forEach((row) => ((row as { UserPreferencesLightningExperiencePreferred?: boolean | null }).UserPreferencesLightningExperiencePreferred = null)));
    const { findings, metrics } = analyseExtracts(changed);
    assert.equal(metric(metrics, "adoption.lightning_share").status, "not_measurable");
    assert.equal(one(findings, "adp.classic-users").status, "not_measurable");
  });

  test("users whose setting is empty are left out, not counted as Classic", () => {
    const changed = change<Sourced<UserRow[]>>(usersExtractor.id, (u) => u.data.filter((row) => row.UserPreferencesLightningExperiencePreferred === true).slice(0, 2).forEach((row) => ((row as { UserPreferencesLightningExperiencePreferred?: boolean | null }).UserPreferencesLightningExperiencePreferred = null)));
    const m = metric(analyseExtracts(changed).metrics, "adoption.lightning_share") as Extract<Metric, { status: "measured" }>;
    assert.equal(m.n, 10);
    assert.equal(m.value, 80);
    assert.ok(m.exclusions.some((e) => /2 users whose setting couldn't be read/.test(e)));
  });

  test("no login history: the login rules and metrics say why", () => {
    const changed = clone(extracts);
    changed.set(LOGINS, gone);
    const { findings, metrics } = analyseExtracts(changed);
    for (const id of ["adp.falling-adoption", "adp.low-engagement-group"]) assert.equal(one(findings, id).status, "not_measurable", id);
    for (const id of ["adoption.daily_active_users", "adoption.weekly_active_users", "adoption.monthly_active_users", "adoption.monthly_active_share", "adoption.mobile_login_share"]) {
      const m = metric(metrics, id);
      assert.equal(m.status, "not_measurable", id);
      assert.match(m.status === "not_measurable" ? m.reason : "", /Login history couldn't be read/);
    }
    // Lightning and reports don't need the login history.
    assert.equal(one(findings, "adp.classic-users").status, "issue");
    valueOf(metric(metrics, "adoption.lightning_share"));
  });

  test("no users: nothing about people is measured", () => {
    const changed = clone(extracts);
    changed.set(usersExtractor.id, gone);
    const { findings, metrics } = analyseExtracts(changed);
    for (const id of ["adp.falling-adoption", "adp.low-engagement-group", "adp.classic-users"]) assert.equal(one(findings, id).status, "not_measurable", id);
    for (const id of ["adoption.daily_active_users", "adoption.lightning_share"]) assert.equal(metric(metrics, id).status, "not_measurable", id);
  });

  test("history that covers fewer than 4 whole months gives no trend", () => {
    const changed = change<Sourced<LoginActivity>>(LOGINS, (l) => {
      l.data.truncated = true;
      l.data.earliest = "2026-08-15T09:00:00.000Z";
    });
    const { findings, metrics } = analyseExtracts(changed);
    const f = one(findings, "adp.falling-adoption");
    assert.equal(f.status, "not_measurable");
    assert.match(f.summary, /covers 1 whole month, and a trend needs 4/);
    // A single whole month is still enough for last month's figure.
    valueOf(metric(metrics, "adoption.monthly_active_users"));
  });

  test("the Lightning setting missing from the org", () => {
    const changed = change<Sourced<UserRow[]>>(usersExtractor.id, (u) => (u.missingFields = ["UserPreferencesLightningExperiencePreferred"]));
    const { findings, metrics } = analyseExtracts(changed);
    assert.equal(one(findings, "adp.classic-users").status, "not_measurable");
    assert.equal(metric(metrics, "adoption.lightning_share").status, "not_measurable");
  });

  test("no channel fields: the mobile share isn't guessed", () => {
    const changed = change<Sourced<LoginActivity>>(LOGINS, (l) => (l.data.channelFields = []));
    assert.equal(metric(analyseExtracts(changed).metrics, "adoption.mobile_login_share").status, "not_measurable");
  });
});

describe("how a login is read", () => {
  test("each channel, first match wins", () => {
    assert.equal(channelOf({ ApiType: "SOAP Partner", LoginType: "Application" }), "api");
    assert.equal(channelOf({ LoginType: "OtherApi" }), "api");
    assert.equal(channelOf({ Application: "Salesforce for iOS", LoginType: "Remote Access 2.0", Platform: "iPhone" }), "mobile app");
    assert.equal(channelOf({ Application: "Some App", LoginType: "Remote Access 2.0", Platform: "Android 14" }), "mobile app");
    assert.equal(channelOf({ LoginType: "SAML Sfdc Initiated SSO" }), "single sign-on");
    assert.equal(channelOf({ LoginType: "Application", AuthenticationServiceId: "0SO000000000001" }), "single sign-on");
    assert.equal(channelOf({ LoginType: "Application", Application: "Browser", Platform: "Mac OSX" }), "browser");
    assert.equal(channelOf({ LoginType: "Remote Access 2.0", Application: "Some Integration" }), "other");
    assert.equal(channelOf({}), "other");
  });

  const activity = (over: Partial<LoginActivity>): LoginActivity => ({ from: "2025-10-31", earliest: null, truncated: false, rowsRead: 0, failedOrOther: 0, days: {}, channels: { browser: 0, "mobile app": 0, "single sign-on": 0, api: 0, other: 0 }, channelFields: [], ...over });
  const now = new Date("2026-03-15T12:00:00Z");

  test("history from the window's start covers every whole month after it", () => {
    assert.deepEqual(coveredMonths(activity({ earliest: "2025-11-02T08:00:00.000Z" }), now), ["2025-11", "2025-12", "2026-01", "2026-02"]);
  });

  test("history that starts later, or was cut off, starts at the next whole month", () => {
    assert.deepEqual(coveredMonths(activity({ earliest: "2025-12-20T08:00:00.000Z" }), now), ["2026-01", "2026-02"]);
    assert.deepEqual(coveredMonths(activity({ earliest: "2025-11-02T08:00:00.000Z", truncated: true }), now), ["2025-12", "2026-01", "2026-02"]);
    assert.deepEqual(coveredMonths(activity({ earliest: "2026-01-01T00:00:00.000Z", truncated: true }), now), ["2026-01", "2026-02"]);
  });

  test("no logins, no months", () => {
    assert.deepEqual(coveredMonths(activity({}), now), []);
  });
});

describe("in the raw notes", () => {
  const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, intake: null });

  test("an Adoption section numbers every adoption metric, and the pipeline metrics leave them out", () => {
    const sections = noteSections(report);
    const adoption = sections.find((s) => s.key === "adoption")!;
    assert.deepEqual(adoption.items.map((i) => i.id), ADOPTION_METRIC_IDS);
    assert.ok(!sections.find((s) => s.key === "pipeline_metrics")!.items.some((i) => i.id.startsWith("adoption.")));
  });

  test("the Word notes print each adoption metric under its reference", async () => {
    const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    const section = noteSections(report).find((s) => s.key === "adoption")!;
    assert.match(text, new RegExp(`${section.ref} +Adoption`));
    assert.match(text, new RegExp(`${section.items[0]!.ref.replace(".", "\\.")} +Daily active users`));
  });
});
