import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { join, relative } from "node:path";
import type * as TS from "typescript";

// What the compiler says every said piece is (DECISIONS.md 657, found in review).
// org-words-pieces.test.ts reads pieces as text; this asks the compiler for each
// one's type, so a piece is a number, a Said, org words, a string literal type,
// or a union of those, and never any, unknown, never or a plain string, whatever
// it is named. A piece whose type holds a string literal and isn't a literal as
// written is listed below with its type, so a literal type widened to take a
// value from the org changes its line here and fails until review reads it.

const LITERAL_PIECES: string[] = [
  // Our names for a segment's dimension: a key of the rule's own as const table, or org words for any other.
  "src/domains/activity/index.ts: dimensionWords(hotspots[0]!.dimension) :: OrgWords | \"team\" | \"rep\" | \"task type\"",
  "src/domains/activity/index.ts: dimensionWords(worst.dimension) :: OrgWords | \"team\" | \"rep\" | \"task type\"",
  "src/domains/adoption/index.ts: g.dimension :: \"profile\" | \"role\"",
  "src/domains/adoption/index.ts: x.g.dimension :: \"profile\" | \"role\"",
  "src/domains/current-pipeline/index.ts: dimensionWords(worst!.dimension) :: OrgWords | \"record type\" | \"team\" | \"rep\" | \"stage\"",
  "src/domains/current-pipeline/index.ts: dimensionWords(worst.dimension) :: OrgWords | \"record type\" | \"team\" | \"rep\" | \"stage\"",
  "src/domains/historic-pipeline/index.ts: dimensionWords(dimension) :: OrgWords | \"type\" | \"record type\" | \"team\" | \"rep\" | \"lead source\" | \"closed quarter\"",
  "src/domains/historic-pipeline/process.ts: dimensionWords(h.dimension) :: OrgWords | \"type\" | \"record type\" | \"team\" | \"rep\" | \"lead source\" | \"closed quarter\"",
  "src/domains/service/operations.ts: dimensionWords(worst!.dimension) :: OrgWords | \"priority\" | \"origin\" | \"owner type\" | \"milestone type\" | \"queue\"",
  "src/domains/service/operations.ts: dimensionWords(worst.dimension) :: OrgWords | \"priority\" | \"origin\" | \"owner type\" | \"milestone type\" | \"queue\"",
  "src/domains/service/operations.ts: dimensionWords(worst.dimension) :: OrgWords | \"priority\" | \"origin\" | \"owner type\" | \"milestone type\" | \"queue\"",
  // Our two watched metrics' names, or org words for any other label.
  "src/domains/historic-pipeline/index.ts: metricWords(metric.label) :: OrgWords | \"Win rate\" | \"Slippage rate\"",
  "src/domains/historic-pipeline/index.ts: metricWords(metric.label) :: OrgWords | \"Win rate\" | \"Slippage rate\"",
  // Fixed sentences and names of our own, declared as string literals (notes.ts, shared.ts, integrations' licence name).
  "src/domains/automation/index.ts: MIGRATE_TO_FLOW_NOTE :: \"Salesforce's Migrate to Flow tool handles the simple cases, but it isn't always a like-for-like move.\"",
  "src/domains/automation/index.ts: MIGRATE_TO_FLOW_NOTE :: \"Salesforce's Migrate to Flow tool handles the simple cases, but it isn't always a like-for-like move.\"",
  "src/domains/automation/shared.ts: count === 1 ? UNNAMED : \"custom objects we couldn't name\" :: \"a custom object we couldn't name\" | \"custom objects we couldn't name\"",
  "src/domains/integrations/index.ts: INTEGRATION_LICENCE :: \"Salesforce Integration\"",
  "src/domains/integrations/index.ts: INTEGRATION_LICENCE :: \"Salesforce Integration\"",
  "src/domains/integrations/index.ts: INTEGRATION_LICENCE :: \"Salesforce Integration\"",
  "src/domains/technical-health/index.ts: o.label === null ? UNNAMED : org(o.label) :: OrgWords | \"a custom object we couldn't name\"",
  // Fixed lists of standard objects, as const: the objects a rule checks, not read from the org.
  "src/domains/configuration/index.ts: o :: \"Account\" | \"Contact\" | \"Lead\" | \"Opportunity\"",
  "src/domains/data-quality/index.ts: o :: \"Account\" | \"Contact\" | \"Lead\"",
  "src/domains/data-quality/index.ts: o :: \"Account\" | \"Contact\" | \"Lead\"",
  "src/domains/data-quality/index.ts: o :: \"Account\" | \"Contact\" | \"Lead\"",
  "src/domains/security/index.ts: r.object :: \"Account\" | \"Contact\" | \"Lead\" | \"Opportunity\"",
  // Verbs chosen by a count: helpers typed to return only "is" or "are", "has" or "have", or "it" or "all of them".
  "src/domains/integrations/index.ts: movable === onFull.length ? (movable === 1 ? \"it\" : \"all of them\") : movable :: number | \"it\" | \"all of them\"",
  "src/domains/service/setup.ts: hasHave(dormant.length) :: \"has\" | \"have\"",
  "src/domains/service/setup.ts: hasHave(empty.length) :: \"has\" | \"have\"",
  "src/domains/service/setup.ts: hasHave(idle.length) :: \"has\" | \"have\"",
  "src/domains/service/setup.ts: hasHave(unread) :: \"has\" | \"have\"",
  "src/domains/service/setup.ts: hasHave(unrouted.length) :: \"has\" | \"have\"",
  "src/domains/service/setup.ts: isAre(active.length + activeSla.length) :: \"is\" | \"are\"",
  "src/domains/service/setup.ts: isAre(activeSla.length) :: \"is\" | \"are\"",
  "src/domains/service/setup.ts: isAre(read.rows.length) :: \"is\" | \"are\"",
  "src/domains/service/setup.ts: isAre(spread) :: \"is\" | \"are\"",
  // Rows of our own as const tables or literal unions (the CPQ comparison, a quote's kind, the team or rep peers, the allowances, storage and code kinds, a sharing object), and a stage's category, which the calibration's schema holds to its enum.
  "src/domains/cpq/quoting.ts: f.against :: \"the other teams\" | \"their team-mates\"",
  "src/domains/cpq/quoting.ts: what :: \"quotes\" | \"approvals\"",
  "src/domains/cpq/setup.ts: m.where :: \"opportunity product\" | \"order product\"",
  "src/domains/cpq/setup.ts: w :: \"opportunity product\" | \"order product\"",
  "src/domains/current-pipeline/index.ts: options.subject :: \"rep\" | \"account\"",
  "src/domains/historic-pipeline/cohort.ts: s.category :: \"open\" | \"won\" | \"lost\" | \"no_decision\" | \"excluded\"",
  "src/domains/integrations/index.ts: label :: \"API requests\" | \"background jobs\"",
  "src/domains/integrations/index.ts: r.first :: \"API requests\" | \"background (asynchronous Apex) jobs\"",
  "src/domains/technical-health/index.ts: first ? r.opening : r.label :: \"data storage\" | \"Data storage\" | \"file storage\" | \"File storage\"",
  "src/domains/technical-health/index.ts: k :: \"Apex classes\" | \"Apex triggers\" | \"Active flows\"",
  // Value reasons and assumptions (DECISIONS.md 663): a fixed sentence and our licence's name, a peer kind, and the
  // calibration's upkeep keys and the nouns calibratedHours takes, each a literal union in value.ts.
  'src/domains/current-pipeline/index.ts: options.subject :: "rep" | "account"',
  'src/domains/integrations/index.ts: INTEGRATION_LICENCE :: "Salesforce Integration"',
  'src/domains/integrations/index.ts: INTEGRATION_LICENCE :: "Salesforce Integration"',
  'src/domains/utilisation/investment.ts: describe :: "The seats" | "The unused seats" | "At list price, the seats"',
  'src/domains/value.ts: key :: UpkeepKey',
  'src/domains/value.ts: noun :: "unused report" | "unused custom field"',
  // Decision 743: the nouns, kinds and read names put into the sentences that reach the digest on branches the synthetic org misses.
  'src/domains/activity/index.ts: category :: "lost" | "won"',
  'src/domains/configuration/documented.ts: what :: "Connected apps" | "Connected apps, named credentials and remote site settings" | "Named credentials" | "Remote site settings"',
  'src/domains/configuration/index.ts: object :: "Account" | "Contact" | "Lead" | "Opportunity"',
  'src/domains/current-pipeline/index.ts: options.subject :: "account" | "rep"',
  'src/domains/historic-pipeline/deep-dive.ts: noun :: "closed deals with stage history" | "lost and no decision deals with stage history" | "won deals with stage history" | "won deals"',
  'src/domains/historic-pipeline/deep-dive.ts: noun :: "lead source" | "type"',
  'src/domains/historic-pipeline/metrics.ts: kind :: "stages"',
  'src/domains/historic-pipeline/metrics.ts: outcome :: "lost" | "won"',
  'src/domains/historic-pipeline/process.ts: noun :: "lost and no decision deals" | "won deals"',
  'src/domains/security/index.ts: what :: "Permission set assignments" | "Permission set groups" | "Permission sets" | "Profiles" | "Users"',
  'src/domains/service/operations.ts: what :: "Case history" | "Case milestones" | "Closed cases" | "Open cases"',
  'src/domains/service/paths.ts: dimensionWords(worst!.dimension) :: "origin" | "priority" | "starting status" | "status" | OrgWords',
  'src/domains/service/paths.ts: dimensionWords(worst.dimension) :: "origin" | "priority" | "starting status" | "status" | OrgWords',
  'src/domains/service/paths.ts: dimensionWords(worst.dimension) :: "origin" | "priority" | "starting status" | "status" | OrgWords',
  'src/domains/service/setup.ts: isAre(activeSla.length) :: "are" | "is"',
];

const ROOT = join(import.meta.dirname, "../../..");
const ts = createRequire(join(ROOT, "package.json"))("typescript") as typeof TS;

/** Every said piece outside tests and org-words.ts, with its file, text and type. */
function pieces(): Array<{ at: string; type: TS.Type; shown: string; written: boolean }> {
  const config = ts.parseJsonConfigFileContent(ts.readConfigFile(join(ROOT, "tsconfig.json"), ts.sys.readFile).config, ts.sys, ROOT);
  const program = ts.createProgram(config.fileNames, config.options);
  const checker = program.getTypeChecker();
  const out: Array<{ at: string; type: TS.Type; shown: string; written: boolean }> = [];
  for (const sf of program.getSourceFiles()) {
    const file = relative(ROOT, sf.fileName);
    if (!file.startsWith("src/") || file.includes("__tests__") || file.endsWith(join("engine", "org-words.ts"))) continue;
    const visit = (node: TS.Node): void => {
      if (ts.isTaggedTemplateExpression(node) && node.tag.getText(sf) === "said" && ts.isTemplateExpression(node.template)) {
        for (const span of node.template.templateSpans) {
          const type = checker.getTypeAtLocation(span.expression);
          out.push({ at: `${file}: ${span.expression.getText(sf).replace(/\s+/g, " ")}`, type, shown: checker.typeToString(type, undefined, ts.TypeFormatFlags.NoTruncation), written: literalAsWritten(span.expression) });
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(sf);
  }
  return out;
}

/** Safe as written, as the pieces test reads it: a string literal, org, eng, joinSaid or said made in place, or a choice between those. */
function literalAsWritten(e: TS.Expression): boolean {
  if (ts.isParenthesizedExpression(e)) return literalAsWritten(e.expression);
  if (ts.isStringLiteral(e) || ts.isNoSubstitutionTemplateLiteral(e)) return true;
  if (ts.isTaggedTemplateExpression(e) && e.tag.getText() === "said") return true;
  if (ts.isCallExpression(e) && ["org", "eng", "joinSaid", "passedOn"].includes(e.expression.getText())) return true;
  return ts.isConditionalExpression(e) && literalAsWritten(e.whenTrue) && literalAsWritten(e.whenFalse);
}

/** A number, a Said, org words or a string literal type: what said may take. */
function allowed(t: TS.Type): boolean {
  if (t.isUnion()) return t.types.every(allowed);
  if (t.flags & (ts.TypeFlags.Any | ts.TypeFlags.Unknown | ts.TypeFlags.Never)) return false;
  if (t.flags & ts.TypeFlags.NumberLike) return true;
  if (t.isStringLiteral()) return true;
  const name = t.aliasSymbol?.name ?? t.getSymbol()?.name;
  return name === "Said" || name === "OrgWords" || name === "PassedOn" || (t.isIntersection() && t.types.some((p) => p.getProperties().some((s) => s.name.startsWith("__@SAID"))));
}

const all = pieces();

test("the compiler finds every said piece a number, a Said, org words or a literal type, never any, unknown or a plain string", () => {
  assert.ok(all.length > 1000, `${all.length} pieces`);
  assert.deepEqual(all.filter((p) => !allowed(p.type)).map((p) => `${p.at} :: ${p.shown}`), []);
});

test("every piece typed with a string literal that isn't one as written is listed with its type", () => {
  // A union's members in a set order: the compiler prints them in the order it first met each type, which
  // moves with the order files import each other, and is no widening (DECISIONS.md 718).
  const settled = (line: string): string => {
    const [at, shown] = line.split(" :: ");
    return `${at} :: ${shown!.split(" | ").sort().join(" | ")}`;
  };
  const found = all.filter((p) => !p.written && (p.type.isUnion() ? p.type.types : [p.type]).some((t) => t.isStringLiteral())).map((p) => settled(`${p.at} :: ${p.shown}`)).sort();
  assert.deepEqual(found, LITERAL_PIECES.map(settled).sort());
});
