import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { runPmd } from "../../lint/pmd.ts";
import { fakePmd } from "./fixtures.ts";

// A process that starts PMD's stand-in, which hangs, and then goes away the
// way a worker can mid-run (DECISIONS.md 670): `exit` calls process.exit(75),
// as the recycle's watchdog does; `wait` waits for the test to signal it;
// `handled` waits too, with a SIGTERM handler of its own, as the worker has.
const [root, mode] = process.argv.slice(2) as [string, "exit" | "wait" | "handled"];
const pmd = fakePmd(root);
const temp = join(root, "tmp");
mkdirSync(temp);
if (mode === "handled") process.once("SIGTERM", () => {
  console.log("own handler ran");
  setTimeout(() => process.exit(0), 200);
});
void runPmd([{ kind: "class", name: "Hang", body: "public class Hang { // FAKE_PMD_HANG\n}" }], pmd, { tempBase: temp, timeLimitMs: 60_000 });
const ready = setInterval(() => {
  if (!existsSync(join(pmd.home, "seen.json"))) return;
  clearInterval(ready);
  console.log("ready");
  if (mode === "exit") process.exit(75);
}, 50);
