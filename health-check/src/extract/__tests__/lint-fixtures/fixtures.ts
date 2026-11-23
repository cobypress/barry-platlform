import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { PmdInstall } from "../../lint/pmd.ts";

// Invented code for the PMD and ESLint tests (DECISIONS.md 670). Nothing here
// came from an org. The secret and the "customer" are planted, so a test can
// prove neither reaches anything kept or logged.

export const PLANTED_SECRET = "sk-plantedlint670notarealkey00000000000";
export const PLANTED_CUSTOMER = "Quokkafield";

/** PMD's stand-in flags the query (its message quotes the line, customer and all), the empty catch and the unused variable. */
export const APEX_WITH_HITS = [
  "public class InvoiceSync {",
  "    public void run(List<Account> accounts) {",
  `        List<Account> rows = [SELECT Id FROM Account WHERE Name = '${PLANTED_CUSTOMER} Holdings'];`,
  "        try {",
  `            Integer ${PLANTED_CUSTOMER.toLowerCase()}Rebate = 1;`,
  "        } catch (Exception e) {}",
  `        String apiKey = '${PLANTED_SECRET}';`,
  "    }",
  "}",
].join("\n");

export const APEX_CLEAN = "public with sharing class Clean {\n    public Boolean ok() { return true; }\n}";

export const TRIGGER_WITH_HIT = "trigger AccountTrigger on Account (before insert) {\n    try { AccountHandler.run(); } catch (Exception e) {}\n}";

/** The code's own suppressions, which PMD honours: kept, and marked. */
export const APEX_SUPPRESSED = [
  "@SuppressWarnings('PMD.EmptyCatchBlock')",
  "public class Quiet {",
  "    public void a() { try { go(); } catch (Exception e) {} }",
  `    public void b() { Integer x = 1; } // NOPMD the ${PLANTED_CUSTOMER} team said so`,
  "}",
].join("\n");

/** Over the size bound, so it's never handed over. */
export const APEX_ENORMOUS = `public class Enormous {\n${"    // padding\n".repeat(20_000)}    public void x() { try { go(); } catch (Exception e) {} }\n}`;

/** A private key block: redacted to one line, every later line where it was. */
export const APEX_PRIVATE_KEY = [
  "public class Signer {",
  "    String pem = '-----BEGIN RSA PRIVATE KEY-----",
  "MIIEowIBAAKCAQEAplanted",
  "-----END RSA PRIVATE KEY-----';",
  "    public void x() { try { go(); } catch (Exception e) {} }",
  "}",
].join("\n");

export const JS_WITH_HITS = [
  "import { LightningElement } from 'lwc';",
  "export default class Invoice extends LightningElement {",
  "  connectedCallback() {",
  `    const ${PLANTED_CUSTOMER.toLowerCase()}Total = 1;`,
  "    eval(this.code);",
  "    if (this.a == this.b) { debugger; }",
  "  }",
  "}",
].join("\n");

/** Tries to switch our rules off and another on, and declares a global: all ignored. */
export const JS_WITH_INLINE_CONFIG = [
  "/* eslint-disable */",
  "/* eslint no-eval: off, no-console: error */",
  "/* global zebra */",
  "eval('x'); // eslint-disable-line",
  "console.log(zebra);",
].join("\n");

export const JS_CLEAN = "export const add = (a, b) => a + b;\n";
export const JS_BROKEN = "export function (";

/** A file the planted .eslintrc sits beside, as a later lane's bundle might hold one. */
export const PLANTED_ESLINTRC = JSON.stringify({ root: true, rules: { "no-eval": "off", "no-debugger": "off" }, plugins: ["evil"] });

const FAKE = fileURLToPath(new URL("./fake-pmd.mjs", import.meta.url));

/**
 * A PMD home the step will accept, whose bin/pmd runs the stand-in, and a
 * Java home holding a bin/java that is never run. The stand-in writes what it
 * was given to seen.json in the home.
 */
export function fakePmd(base: string, version = "7.29.0"): PmdInstall & { home: string } {
  const home = join(base, `pmd-bin-${version}`);
  mkdirSync(join(home, "bin"), { recursive: true });
  mkdirSync(join(home, "lib"), { recursive: true });
  writeFileSync(join(home, "lib", `pmd-core-${version}.jar`), "");
  writeFileSync(join(home, "lib", `pmd-apex-${version}.jar`), "");
  const bin = join(home, "bin", "pmd");
  writeFileSync(bin, `#!/bin/sh\nexec "${process.execPath}" "${FAKE}" "${home}" "$@"\n`);
  chmodSync(bin, 0o755);
  const javaHome = join(base, "java");
  mkdirSync(join(javaHome, "bin"), { recursive: true });
  writeFileSync(join(javaHome, "bin", "java"), "#!/bin/sh\nexit 1\n");
  chmodSync(join(javaHome, "bin", "java"), 0o755);
  return { bin, javaHome, home };
}
