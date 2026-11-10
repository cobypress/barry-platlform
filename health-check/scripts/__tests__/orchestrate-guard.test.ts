import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";
import { verdict } from "../orchestrate-guard.ts";

const bash = (command: string) => verdict({ tool_name: "Bash", tool_input: { command } });
const tool = (tool_name: string, file_path: string) => verdict({ tool_name, tool_input: { file_path } });
const ENGINE = "/Users/x/Desktop/Black Cloud/worktrees/hc-lane-a/health-check";

describe("the orchestration guard blocks what stays Coby's", () => {
  for (const command of [
    "git push origin health-check",
    "cd x && git -C ../repo push",
    "npm test && git push",
    "vercel --prod",
    "npx vercel deploy",
    "npm publish",
    "gh pr create --fill",
    "gh release create v1",
    "sf org list",
    "cd x; sfdx force:org:list",
    "npm run hc -- run --org acme --calibration c.json",
    "node src/cli/hc.ts facts --org acme",
    'NEON_URL="$(neon connection-string production)" node apply.cjs',
    "psql $DATABASE_URL",
    "security find-generic-password -s x",
    "printenv",
    "env",
    "ANTHROPIC_API_KEY=sk-ant-123 npm run hc",
    "echo $SLACK_BOT_TOKEN",
    "cat .env.local",
    "cat /repo/blackcloud-next/.env",
    'ls "/Users/x/Desktop/Clients Orgs"',
    `cat ${homedir()}/.barometer/oauth-client.json`,
    `sed -i '' 's/a/b/' ${ENGINE}/src/salesforce/allowlist.ts`,
    `echo x > ${ENGINE}/scripts/orchestrate-guard.ts`,
  ]) {
    test(`blocks: ${command}`, () => assert.ok(bash(command), command));
  }

  for (const [name, path] of [
    ["Read", "/repo/blackcloud-next/.env.local"],
    ["Read", "/Users/x/Desktop/Clients Orgs/acme/notes.md"],
    ["Read", `${homedir()}/.barometer/oauth-client.json`],
    ["Edit", `${ENGINE}/src/salesforce/allowlist.ts`],
    ["Write", `${ENGINE}/src/salesforce/__tests__/read-only-guarantee.test.ts`],
    ["Edit", `${ENGINE}/src/privacy/__tests__/privacy.test.ts`],
    ["Edit", "/x/worktrees/console-a/lib/health-check/__tests__/connect-crypto.test.ts"],
    ["Write", `${ENGINE}/scripts/orchestrate-settings.json`],
    ["Edit", "/x/barry-platlform/health-check/.claude/settings.json"],
  ] as const) {
    test(`blocks ${name} on ${path}`, () => assert.ok(tool(name, path)));
  }
});

describe("the orchestration guard lets ordinary work through", () => {
  for (const command of [
    "npm test",
    "npm run typecheck && node --test src/render/__tests__/carry-over.test.ts",
    "git -C ../barry-platlform log --oneline -5",
    "git commit -m 'new - health check - handbook'",
    "git merge --no-ff hc/handbook",
    "git worktree add ../worktrees/hc-a -b hc/a health-check",
    "npm run fixtures",
    "npx playwright test",
    "grep -rn SLACK_BOT_TOKEN lib",
    "cat .env.example",
    "lsof -i :3100",
    "ls ~/Desktop/'Black Cloud'/orchestration/health-check",
    `cat ${ENGINE}/src/salesforce/allowlist.ts`,
    "node --test src/salesforce/__tests__/allowlist.test.ts",
  ]) {
    test(`allows: ${command}`, () => assert.equal(bash(command), null));
  }

  for (const [name, path] of [
    ["Read", `${ENGINE}/src/salesforce/allowlist.ts`],
    ["Read", `${ENGINE}/src/privacy/__tests__/privacy.test.ts`],
    ["Edit", `${ENGINE}/src/render/carry-over.ts`],
    ["Write", `${ENGINE}/src/domains/configuration/handbook.ts`],
    ["Write", "/x/blackcloud-next/.env.example"],
  ] as const) {
    test(`allows ${name} on ${path}`, () => assert.equal(tool(name, path), null));
  }
});

describe("as a hook", () => {
  const script = fileURLToPath(new URL("../orchestrate-guard.ts", import.meta.url));
  const run = (stdin: string) => spawnSync(process.execPath, [script], { input: stdin, encoding: "utf8" });

  test("exits 2 with the reason when it blocks", () => {
    const out = run(JSON.stringify({ tool_name: "Bash", tool_input: { command: "git push" } }));
    assert.equal(out.status, 2);
    assert.match(out.stderr, /git push is Coby's/);
  });
  test("exits 0 when it allows", () => {
    assert.equal(run(JSON.stringify({ tool_name: "Bash", tool_input: { command: "npm test" } })).status, 0);
  });
  test("blocks when it can't read its input", () => {
    assert.equal(run("not json").status, 2);
  });
});
