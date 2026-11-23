// A stand-in for PMD 7.29.0's command line, so the suite needs no Java
// (DECISIONS.md 670). It reads the same arguments, writes a report in the shape
// real PMD 7.29.0 writes (checked against it once by hand), and, like real PMD,
// quotes names and code in its messages, so a test can prove none is kept.
// Markers in the invented code make it misbehave. It writes what it was given
// to seen.json beside its home, so a test can see what PMD would have seen.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const home = process.argv[2];
const args = process.argv.slice(3);
const arg = (name) => args[args.indexOf(name) + 1];
const dir = arg("--dir");
const reportFile = arg("--report-file");
const seen = { args, env: { ...process.env }, cwd: process.cwd(), files: {} };
for (const name of readdirSync(dir).sort()) seen.files[name] = readFileSync(join(dir, name), "utf8");
writeFileSync(join(home, "seen.json"), JSON.stringify(seen));

const all = Object.values(seen.files).join("\n");
if (all.includes("FAKE_PMD_CRASH")) {
  // Real PMD's errors can quote the code: the step must never keep this.
  process.stderr.write(all);
  process.stdout.write(all);
  process.exit(1);
}
if (all.includes("FAKE_PMD_HANG")) setInterval(() => {}, 1_000_000);
else {
  const files = [];
  const suppressed = [];
  const processingErrors = [];
  const violation = (line, column, rule, ruleset, priority, description) => ({ beginline: line, begincolumn: column, endline: line, endcolumn: column + 5, description, rule, ruleset, priority, externalInfoUrl: "https://docs.pmd-code.org/example" });
  for (const [name, text] of Object.entries(seen.files)) {
    const filename = join(dir, name);
    if (text.includes("FAKE_PMD_PARSE_ERROR")) {
      processingErrors.push({ filename, message: `ParseException: ${text}`, detail: text });
      continue;
    }
    const found = [];
    const hidden = [];
    const classSuppress = /@SuppressWarnings\('PMD\.EmptyCatchBlock'\)/.test(text);
    text.split("\n").forEach((line, i) => {
      const n = i + 1;
      const here = [];
      if (/catch\s*\([^)]*\)\s*\{\s*\}/.test(line)) here.push(violation(n, line.indexOf("catch") + 1, "EmptyCatchBlock", "Error Prone", 3, "Avoid empty catch blocks"));
      if (/\[\s*SELECT/i.test(line)) here.push(violation(n, line.indexOf("[") + 1, "ApexCRUDViolation", "Security", 3, `Validate CRUD permission before SOQL/DML operation: ${line.trim()}`));
      const unused = /\b(?:Integer|String)\s+(\w+)\s*=/.exec(line);
      if (unused) here.push(violation(n, unused.index + 1, "UnusedLocalVariable", "Best Practices", 5, `Variable '${unused[1]}' defined but not used`));
      if (line.includes("FAKE_PMD_FOREIGN_RULE")) here.push(violation(n, 1, "ApexDoc", "Documentation", 3, "Missing ApexDoc comment"));
      if (line.includes("FAKE_PMD_MANY")) for (let k = 0; k < 120; k++) here.push(violation(n, k + 1, "AvoidHardcodingId", "Error Prone", 3, `Hardcoding Ids: ${line.trim()}`));
      const nopmd = /\/\/\s*NOPMD\s*(.*)$/.exec(line);
      for (const v of here) {
        if (nopmd) hidden.push({ ...v, suppressiontype: "nopmd", usermsg: nopmd[1] });
        else if (classSuppress && v.rule === "EmptyCatchBlock") hidden.push({ ...v, suppressiontype: "annotation" });
        else found.push(v);
      }
    });
    if (found.length) files.push({ filename, violations: found });
    if (hidden.length) suppressed.push({ filename, violations: hidden });
  }
  const report = {
    formatVersion: 0,
    pmdVersion: all.includes("FAKE_PMD_WRONG_VERSION") ? "7.28.0" : "7.29.0",
    timestamp: new Date().toISOString(),
    files,
    suppressedViolations: suppressed,
    processingErrors,
    configurationErrors: [],
  };
  let out = JSON.stringify(report, null, 2);
  if (all.includes("FAKE_PMD_GARBAGE")) out = `not json ${all}`;
  if (all.includes("FAKE_PMD_FLOOD")) out = `${out}\n${" ".repeat(400_000)}`;
  writeFileSync(reportFile, out);
}
