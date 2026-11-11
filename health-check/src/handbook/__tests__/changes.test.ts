import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { SetupAuditChange, SetupAuditSummary } from "../../extract/configuration.ts";
import type { Sourced } from "../../extract/types.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { loadPseudonymMap } from "../../render/render-run.ts";
import type { Finding } from "../../schemas/index.ts";
import { docxText } from "../../testing/docx-text.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CHANGES_DELEGATE, CHANGES_MARKER, CHANGES_PLANTED, SETUP_AUDIT, userId } from "../../testing/synthetic-org.ts";
import { HANDBOOK_FILE, recentChanges } from "../build.ts";
import { renderHandbookDocx } from "../docx.ts";
import { readHandbook } from "../read.ts";
import { CHANGES_NOT_KEPT, Handbook, HANDBOOK_VERSION, MAX_TEXT, type HandbookChange } from "../schema.ts";
import { olderComponents } from "./older-files.ts";

// The handbook's recent changes, from the Setup Audit Trail (DECISIONS.md 461
// and 462). Who made each is a token in handbook.json, and a name only in the
// Word document.

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const changesOf = (dir: string) => {
  const part = Handbook.parse(read(dir, HANDBOOK_FILE)).recentChanges;
  return part.status === "ok" ? part : assert.fail(`recent changes unavailable: ${part.reason}`);
};
const OPTIONS = { clientName: "Synthetic Co", runDate: AS_OF };
const TOKEN = /\[\[[a-z]+:[A-Z]\d{4,}\]\]/;

async function simulated(options: SimulatorOptions = {}, outDir = tempDir(), resume?: string) {
  return runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport: new Simulator(options), log: () => {}, resume });
}

/** Every token handbook.json holds, and the run's map. */
const tokensIn = (dir: string): string[] => [...readFileSync(join(dir, HANDBOOK_FILE), "utf8").matchAll(/\[\[user:U\d{4,}\]\]/g)].map((m) => m[0]);
const mapOf = (dir: string): PseudonymEntry[] => read<PseudonymEntry[]>(dir, "pseudonym-map.json");

describe("the recorded synthetic org's recent changes", () => {
  let dir: string;
  before(async () => {
    dir = (await runFromRecording()).dir;
  });

  test("are its changes that aren't user administration, newest first, with the eight left out counted", () => {
    const part = changesOf(dir);
    const kept = SETUP_AUDIT.filter((r) => r.Section !== "Manage Users");
    assert.equal(part.items.length, kept.length);
    assert.equal(part.total, 30);
    assert.equal(part.leftOutUserAdmin, 8);
    assert.deepEqual([part.cut, part.days], [false, 180]);
    assert.ok(part.items.every((c) => c.section !== "Manage Users"));
    const at = part.items.map((c) => c.at);
    assert.deepEqual(at, [...at].sort().reverse());
    assert.deepEqual([...at].sort(), kept.map((r) => r.CreatedDate).sort());
  });

  test("the setup audit step's summary is worked out as before, so the setup change finding doesn't move", () => {
    const audit = read<Sourced<SetupAuditSummary>>(dir, "checkpoints/setup-audit.json");
    assert.ok(audit.status === "ok");
    const { changes: _changes, ...summary } = audit.data;
    assert.deepEqual(summary, {
      rows: 38,
      earliest: SETUP_AUDIT.map((r) => r.CreatedDate).sort()[0],
      bySection: { Flows: 27, "Customize Opportunities": 3, "Manage Users": 8 },
      configChangesByUser: { [userId(4)]: 27, [userId(9)]: 3 },
      excludedUserAdmin: 8,
    });
    const finding = read<Finding[]>(dir, "findings.json").find((f) => f.ruleId === "cfg.setup-change-concentration")!;
    assert.deepEqual(finding.facts, { changes: 30, topShare: 90, byInactive: 3 });
  });

  test("each says who made it as a token the run's pseudonym map holds", () => {
    const part = changesOf(dir);
    const map = new Map(mapOf(dir).map((e) => [e.token, e]));
    for (const c of part.items) assert.ok(c.by !== null && map.has(c.by), `${c.by}`);
    assert.equal(map.get(part.items.find((c) => c.section === "Flows")!.by!)!.name, "Jo Ellis");
    assert.equal(map.get(part.items.find((c) => c.section === "Customize Opportunities")!.by!)!.name, "Kim Fox");
    // A token, never the name.
    assert.ok(!readFileSync(join(dir, HANDBOOK_FILE), "utf8").includes("Jo Ellis"));
  });

  test("keeps who someone was logged in as, Salesforce's sentence redacted, and a long one cut and marked", () => {
    const part = changesOf(dir);
    assert.equal(part.items[0]!.loggedInAs, CHANGES_DELEGATE);
    assert.ok(part.items.slice(1).every((c) => c.loggedInAs === null));
    const planted = part.items.find((c) => c.what?.includes(CHANGES_MARKER))!;
    assert.equal(planted.what, `Changed flow description to "Sync to ${CHANGES_MARKER}" api_key = "[REDACTED]"`);
    assert.ok(!JSON.stringify(part).includes(CHANGES_PLANTED));
    const long = part.items.filter((c) => c.whatCut);
    assert.equal(long.length, 1);
    assert.equal(long[0]!.what!.length, MAX_TEXT);
    assert.ok(long[0]!.whatLength! > MAX_TEXT);
  });

  test("its document names each person, says who was logged in as them, and holds no token", async () => {
    const h = Handbook.parse(read(dir, HANDBOOK_FILE));
    const text = docxText(await renderHandbookDocx(h, { ...OPTIONS, pseudonyms: loadPseudonymMap(dir) }));
    const part = text.slice(text.indexOf("## Recent changes"));
    assert.match(part, /\n30 Oct 2026 at 12:00 UTC, Jo Ellis \(support\.desk@example\.com logged in as them\): Flows {2}activatedinteractiondefversion\nActivated flow version #2 of Lead Router\n/);
    assert.match(part, /, Kim Fox: Customize Opportunities /);
    assert.match(part, /\[cut: the first 4,000 of 4,583 characters\]/);
    assert.match(part, new RegExp(`Sync to ${CHANGES_MARKER}`));
    assert.doesNotMatch(text, TOKEN);
    // Month headings, newest first.
    assert.deepEqual([...part.matchAll(/^### (.+)$/gm)].map((m) => m[1]), ["October 2026", "September 2026", "August 2026", "July 2026", "June 2026"]);
    assert.match(text, /\nRecent changes: 30 setup changes from 22 Jun 2026 to 30 Oct 2026\. Salesforce keeps this trail for 180 days\.\n/);
    assert.match(part, /We left out 8 changes to users, such as password resets and email changes/);
  });

  test("without the run's pseudonym map, the document names no one and shows no token", async () => {
    const h = Handbook.parse(read(dir, HANDBOOK_FILE));
    for (const pseudonyms of [undefined, null]) {
      const text = docxText(await renderHandbookDocx(h, { ...OPTIONS, pseudonyms }));
      assert.doesNotMatch(text, TOKEN);
      assert.ok(!text.includes("Jo Ellis"));
      assert.match(text, /30 Oct 2026 at 12:00 UTC, a user whose name we couldn't look up \(support\.desk@example\.com logged in as them\): Flows/);
    }
  });
});

describe("a run resumed after the step's version went up", () => {
  // The step reads the trail again, with the run's own date (DECISIONS.md 463).
  const LATER = "HB-LATER-0463";
  const later = { Action: "changedinteractiondef", Section: "Customize Opportunities", Display: `Changed field Stage ${LATER}`, CreatedDate: new Date(new Date(AS_OF).getTime() + 7_200_000).toISOString().replace("Z", "+0000"), CreatedById: userId(9), CreatedBy: { Name: "Kim Fox" }, DelegateUser: null };
  type Steps = { steps: Array<{ id: string; version: number }> };
  const summaryOf = (dir: string) => {
    const audit = read<Sourced<SetupAuditSummary>>(dir, "checkpoints/setup-audit.json");
    assert.ok(audit.status === "ok");
    const { changes, ...summary } = audit.data;
    return { summary, changes };
  };
  const factsOf = (dir: string) => read<Finding[]>(dir, "findings.json").find((f) => f.ruleId === "cfg.setup-change-concentration")!.facts;

  test("doesn't take in a change made after the run's date: the summary, the finding and the handbook are as the first read left them", async () => {
    const first = await simulated();
    const was = { ...summaryOf(first.dir), facts: factsOf(first.dir), items: changesOf(first.dir).items };
    assert.ok(was.changes.length > 0);

    // As a run in flight at deployment left it: the step at version 1, whose checkpoint kept no changes.
    const run = read<Steps>(first.dir, "run.json");
    run.steps.find((s) => s.id === "setup-audit")!.version = 1;
    writeFileSync(join(first.dir, "run.json"), JSON.stringify(run));
    const checkpoint = read<{ data: Partial<SetupAuditSummary> }>(first.dir, "checkpoints/setup-audit.json");
    delete checkpoint.data.changes;
    writeFileSync(join(first.dir, "checkpoints/setup-audit.json"), JSON.stringify(checkpoint));

    // The org has since gained a change, two hours after the run's date, and the resume is later still.
    const transport = new Simulator({ setupAudit: [later, ...SETUP_AUDIT] });
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: join(first.dir, ".."), now: new Date(), transport, log: () => {}, resume: first.run.id });
    assert.ok(transport.requests.some((r) => decodeURIComponent(r.url).includes("FROM SetupAuditTrail")), "the step read the trail again");
    assert.equal(read<Steps>(first.dir, "run.json").steps.find((s) => s.id === "setup-audit")!.version, 2);

    const now = summaryOf(first.dir);
    assert.deepEqual(now.summary, was.summary);
    assert.deepEqual(now.changes, was.changes);
    assert.deepEqual(factsOf(first.dir), was.facts);
    assert.deepEqual(changesOf(first.dir).items, was.items);
    for (const file of ["checkpoints/setup-audit.json", "findings.json", HANDBOOK_FILE]) assert.ok(!readFileSync(join(first.dir, file), "utf8").includes(LATER), file);
  });
});

describe("who made a change", () => {
  const OUTSIDE = userId(990);
  const base = { Action: "changedinteractiondef", Section: "Flows", DelegateUser: null };
  const rows = [
    // Salesforce's own: no user at all.
    { ...base, Display: "Changed flow Invoice Reminder", CreatedDate: "2026-10-20T09:30:00.000+0000", CreatedById: null, CreatedBy: null },
    // Someone the run's user list doesn't hold, named by the trail.
    { ...base, Display: "Changed flow Lead Router", CreatedDate: "2026-10-21T09:30:00.000+0000", CreatedById: OUTSIDE, CreatedBy: { Name: "Robin Vale" } },
    // Nothing but a date and a user.
    { Action: null, Section: null, Display: null, DelegateUser: null, CreatedDate: "2026-10-22T09:30:00.000+0000", CreatedById: userId(4), CreatedBy: { Name: "Jo Ellis" } },
    // Text that looks like a token is Salesforce's, and prints as written.
    { ...base, Display: "Renamed flow to [[user:U0004]] Review", CreatedDate: "2026-10-19T09:30:00.000+0000", CreatedById: userId(4), CreatedBy: { Name: "Jo Ellis" } },
  ];
  let first: { dir: string; runId: string };

  before(async () => {
    const result = await simulated({ setupAudit: rows });
    first = { dir: result.dir, runId: result.run.id };
  });

  test("a change with no user has none, and one outside the user list is labelled from the trail's name", () => {
    const part = changesOf(first.dir);
    assert.equal(part.items.length, 4);
    const byDisplay = new Map(part.items.map((c) => [c.what, c]));
    assert.equal(byDisplay.get("Changed flow Invoice Reminder")!.by, null);
    const outside = mapOf(first.dir).find((e) => e.id === OUTSIDE)!;
    assert.equal(outside.name, "Robin Vale");
    assert.equal(byDisplay.get("Changed flow Lead Router")!.by, outside.token);
    const bare = byDisplay.get(null)!;
    assert.deepEqual([bare.section, bare.action, bare.whatCut, bare.whatLength], [null, null, false, null]);
  });

  test("every token in handbook.json is in the run's map", () => {
    const tokens = new Set(mapOf(first.dir).map((e) => e.token));
    assert.ok(tokensIn(first.dir).length >= 3);
    for (const token of tokensIn(first.dir)) assert.ok(tokens.has(token), token);
  });

  test("and still is when the run is resumed, even from a map that lost one", async () => {
    const before = readFileSync(join(first.dir, HANDBOOK_FILE), "utf8");
    const outDir = join(first.dir, "..");
    await simulated({ setupAudit: rows }, outDir, first.runId);
    assert.equal(readFileSync(join(first.dir, HANDBOOK_FILE), "utf8"), before, "the same tokens");
    // A map that lost the outside user: the resumed run labels them again.
    writeFileSync(join(first.dir, "pseudonym-map.json"), JSON.stringify(mapOf(first.dir).filter((e) => e.id !== OUTSIDE)));
    await simulated({ setupAudit: rows }, outDir, first.runId);
    const tokens = new Set(mapOf(first.dir).map((e) => e.token));
    for (const token of tokensIn(first.dir)) assert.ok(tokens.has(token), token);
    assert.equal(mapOf(first.dir).find((e) => e.id === OUTSIDE)!.name, "Robin Vale");
  });

  test("the document names them from the trail, says when no user is recorded, and restores only who made it", async () => {
    const h = Handbook.parse(read(first.dir, HANDBOOK_FILE));
    const text = docxText(await renderHandbookDocx(h, { ...OPTIONS, pseudonyms: loadPseudonymMap(first.dir) }));
    assert.match(text, /\n21 Oct 2026 at 09:30 UTC, Robin Vale: Flows {2}changedinteractiondef\nChanged flow Lead Router\n/);
    assert.match(text, /\n20 Oct 2026 at 09:30 UTC, no user recorded: Flows {2}changedinteractiondef\n/);
    assert.match(text, /\n22 Oct 2026 at 09:30 UTC, Jo Ellis: section not recorded\nSalesforce gave no description of this change\.\n/);
    assert.match(text, /\n19 Oct 2026 at 09:30 UTC, Jo Ellis: Flows {2}changedinteractiondef\nRenamed flow to \[\[user:U0004\]\] Review(\n|$)/);
  });
});

describe("the part on its own", () => {
  const change = (n: number, over: Partial<SetupAuditChange> = {}): SetupAuditChange => ({
    CreatedDate: `2026-10-${String(n).padStart(2, "0")}T08:00:00.000+0000`,
    Section: "Flows",
    Action: "changedinteractiondef",
    Display: `Change ${n}`,
    CreatedById: userId(4),
    CreatedByName: "Jo Ellis",
    DelegateUser: null,
    ...over,
  });
  const audit = (changes: SetupAuditChange[], truncated = false, excludedUserAdmin = 0): Sourced<SetupAuditSummary> => ({
    status: "ok",
    data: { rows: changes.length + excludedUserAdmin, earliest: null, bySection: {}, configChangesByUser: {}, excludedUserAdmin, changes },
    source: { kind: "soql", api: "rest", query: "SELECT … FROM SetupAuditTrail" },
    truncated,
  });
  const label = (id: string) => `[[user:U${id.slice(-7, -3)}]]`;
  const bare = { readBy: { metadata: null, apex: null }, flows: { status: "unavailable", reason: "x" }, validationRules: { status: "unavailable", reason: "x" }, apexClasses: { status: "unavailable", reason: "x" }, apexTriggers: { status: "unavailable", reason: "x" } } as const;
  const doc = async (recent: Handbook["recentChanges"]) => docxText(await renderHandbookDocx(Handbook.parse({ version: HANDBOOK_VERSION, ...bare, recentChanges: recent }), OPTIONS));

  test("says it's cut when the trail had more than the read takes", async () => {
    const part = recentChanges(audit([change(2), change(1)], true, 1_998), label);
    assert.ok(part.status === "ok" && part.cut);
    const text = await doc(part);
    assert.match(text, /\nSalesforce had more changes than we read: we read the newest 2,000, so older ones from the 180 days aren't here\.\n/);
    assert.match(text, /\nRecent changes: the newest 2 setup changes we read, from 1 Oct 2026 to 2 Oct 2026\./);
    assert.ok(recentChanges(audit([change(1)]), label).status === "ok" && !(recentChanges(audit([change(1)]), label) as { cut: boolean }).cut);
  });

  test("a read that failed is unavailable with the read's reason, and says so", async () => {
    const reason = "SetupAuditTrail isn't available in this org, or to the connected user.";
    const part = recentChanges({ status: "unavailable", reason, source: { kind: "soql", api: "rest", query: "x" } }, label);
    assert.deepEqual(part, { status: "unavailable", reason });
    assert.deepEqual(recentChanges(undefined, label), { status: "unavailable", reason: "This run didn't read the org's setup audit trail." });
    assert.match(await doc(part), /\nWe couldn't read your recent setup changes, so this handbook doesn't list them\. SetupAuditTrail isn't available/);
  });

  test("a checkpoint from before the changes were kept says so", () => {
    const old = audit([]);
    delete (old as { data: Partial<SetupAuditSummary> }).data.changes;
    assert.deepEqual(recentChanges(old, label), { status: "unavailable", reason: CHANGES_NOT_KEPT });
  });

  test("none says so, with the user administration left out", async () => {
    const text = await doc(recentChanges(audit([], false, 8), label));
    assert.match(text, /\nRecent changes: none in the last 180 days, apart from 8 changes to users\./);
    assert.match(text, /\nNo setup changes were recorded in the last 180 days, apart from 8 changes to users, which this handbook leaves out\.$/);
  });

  test("a Display past the limit is cut at it and marked, with its full length", () => {
    const part = recentChanges(audit([change(1, { Display: "x".repeat(MAX_TEXT + 10) })]), label);
    assert.ok(part.status === "ok");
    const c: HandbookChange = part.items[0]!;
    assert.deepEqual([c.what!.length, c.whatCut, c.whatLength], [MAX_TEXT, true, MAX_TEXT + 10]);
  });

  test("a key in a Display is redacted here too, whatever the step kept", () => {
    const part = recentChanges(audit([change(1, { Display: `Set the client_secret = "${CHANGES_PLANTED}" on Lead Router` })]), label);
    assert.ok(part.status === "ok");
    assert.equal(part.items[0]!.what, 'Set the client_secret = "[REDACTED]" on Lead Router');
  });

  // The 10,000th user's token has five digits, and restore() names it (decision 464).
  const tenThousand = (): { pseudonyms: Pseudonymiser; token: string } => {
    const pseudonyms = new Pseudonymiser();
    for (let n = 1; n < 10_000; n++) pseudonyms.label("user", `filler-${String(n).padStart(5, "0")}`, "Sales");
    return { pseudonyms, token: pseudonyms.label("user", userId(4), "Jo Ellis") };
  };

  test("a token of five digits, the 10,000th user's, prints as their name", async () => {
    const { pseudonyms, token } = tenThousand();
    assert.equal(token, "[[user:U10000]]");
    const part = recentChanges(audit([change(1)]), () => token);
    assert.ok(part.status === "ok" && part.items[0]!.by === token);
    const text = docxText(await renderHandbookDocx(Handbook.parse({ version: HANDBOOK_VERSION, ...bare, recentChanges: part }), { ...OPTIONS, pseudonyms }));
    assert.match(text, /\n1 Oct 2026 at 08:00 UTC, Jo Ellis: Flows {2}changedinteractiondef\nChange 1(\n|$)/);
    assert.ok(!text.includes(token) && !text.includes("U10000"));
  });

  test("a token of five digits, which the map doesn't restore, prints as a user we couldn't look up and never as the token", async () => {
    const { pseudonyms } = tenThousand();
    const token = "[[user:U10001]]";
    const part = recentChanges(audit([change(1)]), () => token);
    assert.ok(part.status === "ok" && part.items[0]!.by === token);
    const text = docxText(await renderHandbookDocx(Handbook.parse({ version: HANDBOOK_VERSION, ...bare, recentChanges: part }), { ...OPTIONS, pseudonyms }));
    assert.match(text, /\n1 Oct 2026 at 08:00 UTC, a user whose name we couldn't look up: Flows {2}changedinteractiondef\nChange 1(\n|$)/);
    assert.ok(!text.includes(token) && !text.includes("U10001"));
  });

  test("a read that failed on a real run leaves the part unavailable with the step's own reason", async () => {
    const run = await simulated({ failQuery: (soql) => (soql.includes("FROM SetupAuditTrail") ? { errorCode: "INSUFFICIENT_ACCESS", message: "Setup audit trail refused" } : null) });
    const part = Handbook.parse(read(run.dir, HANDBOOK_FILE)).recentChanges;
    const step = read<Sourced<SetupAuditSummary>>(run.dir, "checkpoints/setup-audit.json");
    assert.ok(step.status === "unavailable" && part.status === "unavailable");
    assert.equal(part.reason, step.reason);
  });
});

/** A file's Apex parts as a version 3 or 2 file has them: no last change on any class or trigger, and no word of them on the part. */
function olderApex<T extends Record<string, unknown>>(file: T): T {
  const older = (part: { status: string; items?: Array<Record<string, unknown>>; lastChanges?: string }) => {
    if (part.status !== "ok") return part;
    const { lastChanges: _said, ...rest } = part;
    return { ...rest, items: part.items!.map(({ lastChanged: _gone, ...item }) => item) };
  };
  return { ...file, apexClasses: older(file.apexClasses as never), apexTriggers: older(file.apexTriggers as never) };
}

describe("a handbook.json from before recent changes were kept", () => {
  test("still makes its document, whose last part says the run was read before they were kept", async () => {
    // A version 2 file as runs wrote it: the synthetic org's, without the fifth
    // part, or any class's, trigger's, flow's or rule's last change (decisions 473 and 482).
    const { dir: source } = await runFromRecording();
    const { recentChanges: _gone, ...four } = olderApex(olderComponents(read<Record<string, unknown>>(source, HANDBOOK_FILE)));
    const dir = join(tempDir(), "run");
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify({ ...four, version: 2 }));
    const handbook = readHandbook(dir);
    assert.ok(handbook.ok);
    assert.deepEqual(handbook.handbook.recentChanges, { status: "unavailable", reason: CHANGES_NOT_KEPT });
    const text = docxText(await renderHandbookDocx(handbook.handbook, OPTIONS));
    assert.match(text, /\nRecent changes: this run was read before recent changes were kept\.\n/);
    assert.match(text, /\n## Recent changes\n\nThis run was read before recent changes were kept\.$/);
    assert.doesNotMatch(text, /We couldn't read your recent setup changes/);
    // But not with a fifth part it can't have, nor as version 1.
    for (const wrong of [{ ...four, version: 2, recentChanges: changesOf(source) }, { ...four, version: 1 }]) {
      writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify(wrong));
      assert.deepEqual(readHandbook(dir), { ok: false, why: "format" });
    }
  });
});
