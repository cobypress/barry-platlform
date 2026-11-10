// The unattended loop's PreToolUse hook (docs/orchestration.md, "Waiting for
// Coby"). It runs before every tool call of the orchestrator and every agent,
// and blocks what stays Coby's: pushing, deploying and publishing, Salesforce
// orgs, databases, the real Claude key, secrets and the Keychain, his real data,
// and the files that hold the line (security tests, the allowlist, this guard).
// Exit 2 blocks the call and hands the reason back to the agent.
//
// It's a backstop, not the only line: the settings' deny rules and every
// agent's rules say the same. A miss here costs a mistake; a false block costs
// a brief rewrite, so it errs on the side of blocking.
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";

export type HookInput = { tool_name?: string; tool_input?: Record<string, unknown> };

// Files no agent may change unattended. Paths are matched on their end, so a
// worktree's copy is covered too.
const LOCKED_FILES: RegExp[] = [
  /health-check\/src\/salesforce\/allowlist\.ts$/,
  /health-check\/src\/salesforce\/__tests__\/(allowlist|read-only-guarantee)\.test\.ts$/,
  /health-check\/src\/privacy\/__tests__\/[^/]+\.test\.ts$/,
  /lib\/health-check\/__tests__\/(auth|connect-crypto|microsite)\.test\.ts$/,
  /health-check\/scripts\/orchestrate-[^/]+$/,
  /health-check\/scripts\/__tests__\/orchestrate-guard\.test\.ts$/,
  /\.claude\/settings(\.local)?\.json$/,
];

const home = homedir();
// Real data and secrets: never read, never written.
const PRIVATE_PATHS: Array<{ test: (path: string) => boolean; why: string }> = [
  { test: (p) => /(^|\/)\.env(\.(?!example\b)[\w.-]+)?$/.test(p), why: ".env files hold production secrets" },
  { test: (p) => p.includes("/Clients Orgs/") || p.endsWith("/Clients Orgs"), why: "Clients Orgs is customer data" },
  { test: (p) => p.startsWith(`${home}/.barometer`) || p.includes("/.barometer/"), why: "~/.barometer holds the Salesforce app's secret" },
  { test: (p) => p.startsWith(`${home}/.ssh`) || p.includes("/Library/Keychains"), why: "keys and the Keychain are Coby's" },
  { test: (p) => p.endsWith("Salesforce Health Check Product Plan.md"), why: "the product plan names customers" },
];

// Commands that stay Coby's. Matched against the whole command, so a chain
// (`a && git push`) is caught as well as a lone one.
const AT_START = String.raw`(?:^|[;&|(\n]\s*|\$\(\s*|\b(?:sudo|env|xargs|exec|time|nohup)\s+)`;
const BLOCKED_COMMANDS: Array<{ re: RegExp; why: string }> = [
  { re: /\bgit\b[^;&|\n]*\bpush\b/, why: "git push is Coby's: list the push command in the morning report" },
  { re: new RegExp(`${AT_START}(?:npx\\s+)?(vercel|netlify|render)\\b`), why: "deploys are Coby's" },
  { re: /\b(npm|pnpm|yarn)\s+publish\b/, why: "publishing is Coby's" },
  { re: /\bgh\s+(release|pr\s+(create|merge)|repo\s+create)\b/, why: "releases and pull requests are Coby's" },
  { re: new RegExp(`${AT_START}(sf|sfdx)\\b`), why: "Salesforce orgs are Coby's: use the synthetic org" },
  { re: /\b(npm\s+run\s+hc|hc\.ts)\b[^;&|\n]*\b(run|facts)\b[^;&|\n]*--org\b/, why: "runs against a Salesforce org are Coby's" },
  { re: new RegExp(`${AT_START}(neon|psql|pg_dump|redis-cli)\\b`), why: "databases are Coby's: write the migration as a file" },
  { re: new RegExp(`${AT_START}security\\b`), why: "the Keychain is Coby's" },
  { re: new RegExp(`${AT_START}printenv\\b|${AT_START}env\\s*($|[;&|])`), why: "printing the environment can show secrets" },
  { re: /\b(ANTHROPIC_API_KEY|CLAUDE_API_KEY|[A-Z_]*(?:SECRET|TOKEN|PRIVATE_KEY|_KEY|DATABASE_URL|REDIS_URL|NEON_URL))\s*=\s*\S/, why: "setting a secret or a real key is Coby's" },
  { re: /\$\{?(ANTHROPIC_API_KEY|CLAUDE_API_KEY|[A-Z_]*(?:SECRET|TOKEN|PRIVATE_KEY|DATABASE_URL|REDIS_URL|NEON_URL))\b/, why: "reading a secret is Coby's" },
];

// A shell command that writes to a file: the locked list applies to it too.
const WRITES = /(\bsed\s+-i|\btee\b|\bmv\b|\bcp\b|\brm\b|\btruncate\b|\bperl\s+-p?i|>)/;

const quotedOrBare = (command: string): string[] => command.match(/"[^"]*"|'[^']*'|[^\s"';&|<>()]+/g)?.map((t) => t.replace(/^["']|["']$/g, "")) ?? [];
const expand = (path: string): string => (path.startsWith("~/") ? `${home}${path.slice(1)}` : path);

function checkPath(path: string, writing: boolean): string | null {
  const p = expand(path);
  for (const rule of PRIVATE_PATHS) if (rule.test(p)) return rule.why;
  if (writing && LOCKED_FILES.some((re) => re.test(p))) return "this file holds the line (security test, allowlist or the guard): changing it is Coby's";
  return null;
}

/** Why the call is blocked, or null to let it through. */
export function verdict(input: HookInput): string | null {
  const tool = input.tool_name ?? "";
  const args = input.tool_input ?? {};
  if (tool === "Bash") {
    const command = String(args.command ?? "");
    for (const { re, why } of BLOCKED_COMMANDS) if (re.test(command)) return why;
    const writing = WRITES.test(command);
    for (const token of quotedOrBare(command)) {
      if (!/[/.~]/.test(token)) continue;
      const why = checkPath(token, writing);
      if (why) return why;
    }
    // A path with spaces, such as "Clients Orgs", may not survive tokenising.
    if (command.includes("Clients Orgs")) return "Clients Orgs is customer data";
    return null;
  }
  const writing = ["Write", "Edit", "MultiEdit", "NotebookEdit"].includes(tool);
  for (const key of ["file_path", "path", "notebook_path"]) {
    const value = args[key];
    if (typeof value === "string") {
      const why = checkPath(value, writing);
      if (why) return why;
    }
  }
  return null;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  let input: HookInput;
  try {
    input = JSON.parse(readFileSync(0, "utf8")) as HookInput;
  } catch {
    process.stderr.write("orchestrate-guard: couldn't read the hook input, so the call is blocked\n");
    process.exit(2);
  }
  const why = verdict(input);
  if (why) {
    process.stderr.write(`Blocked by the orchestration guard: ${why}. See docs/orchestration.md, "Waiting for Coby".\n`);
    process.exit(2);
  }
}
