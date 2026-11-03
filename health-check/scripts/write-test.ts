// Proves the Barometer Read Only user can't write. Development org only.
//
//   node scripts/write-test.ts
//
// As the admin (through the Salesforce CLI) it creates one test Account. Then,
// as the read-only user (a JWT bearer token from ~/.barometer/dev), it checks it
// can read that record, and tries to create, update and delete it, plus a few
// other ways in: a Task, an Opportunity, a composite create, anonymous Apex and
// a Metadata API deploy. Every one must be refused. The admin then checks the
// record is untouched and deletes it.
//
// This is deliberately not the engine's client, which has no way to send a
// write. It prints statuses and Salesforce error codes, never a token.
import { execFile } from "node:child_process";
import { createSign } from "node:crypto";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const ALIAS = "bc-hc-dev";
const API = "v68.0";
const CONFIG = join(homedir(), ".barometer", "dev", "bc-hc-dev-jwt.json");
const TEST_NAME = "Barometer write test 0001";

type DevJwt = { clientId: string; username: string; instanceUrl: string; audience: string; privateKeyPath: string };
type Attempt = { name: string; status: number; code: string; message: string; refused: boolean };

const execFileAsync = promisify(execFile);

async function sf(args: string[]): Promise<{ status: number; result?: Record<string, unknown>; message?: string }> {
  try {
    const { stdout } = await execFileAsync("sf", [...args, "--target-org", ALIAS, "--json"], { maxBuffer: 4 * 1024 * 1024 });
    return JSON.parse(stdout);
  } catch (err) {
    const stdout = (err as { stdout?: string }).stdout;
    if (stdout) return JSON.parse(stdout);
    throw new Error(`sf ${args.slice(0, 2).join(" ")} failed`);
  }
}

async function adminQuery<T>(soql: string): Promise<T[]> {
  const out = await sf(["data", "query", "--query", soql]);
  return ((out.result?.records ?? []) as T[]);
}

async function readOnlyToken(config: DevJwt): Promise<{ accessToken: string; instanceUrl: string; userId: string }> {
  const b64 = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${b64({ alg: "RS256" })}.${b64({ iss: config.clientId, sub: config.username, aud: config.audience, exp: Math.floor(Date.now() / 1000) + 180 })}`;
  const signature = createSign("RSA-SHA256").update(unsigned).sign(readFileSync(config.privateKeyPath), "base64url");
  const res = await fetch(`${config.instanceUrl}/services/oauth2/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: `${unsigned}.${signature}` }),
  });
  const body = (await res.json()) as { access_token?: string; instance_url?: string; id?: string; error?: string };
  if (!res.ok || !body.access_token || !body.instance_url) throw new Error(`No token for the read-only user: ${body.error ?? res.status}`);
  return { accessToken: body.access_token, instanceUrl: body.instance_url, userId: body.id?.split("/").at(-1) ?? "" };
}

async function main(): Promise<void> {
  const [org] = await adminQuery<{ OrganizationType: string }>("SELECT OrganizationType FROM Organization");
  if (org?.OrganizationType !== "Developer Edition") throw new Error(`${ALIAS} is not a Developer Edition org. Stopped before any write.`);

  const config = JSON.parse(readFileSync(CONFIG, "utf8")) as DevJwt;
  const created = await sf(["data", "create", "record", "--sobject", "Account", "--values", `Name='${TEST_NAME}'`]);
  const recordId = String(created.result?.id ?? "");
  if (!/^001[A-Za-z0-9]{12,15}$/.test(recordId)) throw new Error(`The admin couldn't create the test Account: ${created.message ?? "no id"}`);
  console.log(`Admin created test Account ${recordId}`);

  const attempts: Attempt[] = [];
  let readOk = false;
  try {
    const token = await readOnlyToken(config);
    console.log(`Read-only user ${token.userId} has a token for ${new URL(token.instanceUrl).hostname}`);
    const call = async (method: string, path: string, body?: string, headers: Record<string, string> = {}): Promise<{ status: number; text: string }> => {
      const res = await fetch(`${token.instanceUrl}${path}`, {
        method,
        headers: { Authorization: `Bearer ${token.accessToken}`, "Content-Type": "application/json", Accept: "application/json", ...headers },
        body,
        redirect: "error",
      });
      return { status: res.status, text: await res.text() };
    };
    const attempt = async (name: string, method: string, path: string, body?: string, headers?: Record<string, string>): Promise<void> => {
      const res = await call(method, path, body, headers);
      const code = res.text.match(/"errorCode"\s*:\s*"([A-Z_]+)"|<faultcode>[^<]*?:?([A-Z_]+)<\/faultcode>|"success"\s*:\s*(false|true)/);
      let refused = res.status >= 400;
      // Anonymous Apex answers 200 with success false when it can't run.
      if (res.status === 200 && /"compiled"/.test(res.text)) refused = /"success"\s*:\s*false/.test(res.text);
      // Composite answers 200 overall, with the subrequest's own status inside.
      if (res.status === 200 && /"compositeResponse"/.test(res.text)) refused = !/"httpStatusCode"\s*:\s*20\d/.test(res.text);
      // Metadata deploy only queues a job. Its result is checked separately below.
      const message = (res.text.match(/"(?:message|compileProblem|exceptionMessage)"\s*:\s*"([^"]+)"|<faultstring>([^<]+)/) ?? []).slice(1).find(Boolean) ?? "";
      attempts.push({ name, status: res.status, code: code?.[1] ?? code?.[2] ?? (code?.[3] ? `success=${code[3]}` : ""), message: message.slice(0, 140), refused });
    };

    const read = await call("GET", `/services/data/${API}/sobjects/Account/${recordId}?fields=Id,Name`);
    readOk = read.status === 200 && read.text.includes(recordId);
    console.log(`Read the test Account: ${read.status}${readOk ? " (visible)" : ""}`);

    await attempt("create Account", "POST", `/services/data/${API}/sobjects/Account`, JSON.stringify({ Name: "Barometer write test 0002" }));
    await attempt("update Account", "PATCH", `/services/data/${API}/sobjects/Account/${recordId}`, JSON.stringify({ Name: "Barometer write test changed" }));
    await attempt("update Account by method override", "POST", `/services/data/${API}/sobjects/Account/${recordId}?_HttpMethod=PATCH`, JSON.stringify({ Name: "Barometer write test changed" }));
    await attempt("delete Account", "DELETE", `/services/data/${API}/sobjects/Account/${recordId}`);
    await attempt("create Task on the Account", "POST", `/services/data/${API}/sobjects/Task`, JSON.stringify({ Subject: "Barometer write test", WhatId: recordId }));
    await attempt("create Opportunity", "POST", `/services/data/${API}/sobjects/Opportunity`, JSON.stringify({ Name: "Barometer write test", StageName: "Prospecting", CloseDate: "2026-12-31", AccountId: recordId }));
    await attempt(
      "composite create Account",
      "POST",
      `/services/data/${API}/composite`,
      JSON.stringify({ allOrNone: true, compositeRequest: [{ method: "POST", url: `/services/data/${API}/sobjects/Account`, referenceId: "a", body: { Name: "Barometer write test 0003" } }] }),
    );
    await attempt("anonymous Apex update", "GET", `/services/data/${API}/tooling/executeAnonymous?anonymousBody=${encodeURIComponent(`update new Account(Id='${recordId}', Name='Barometer write test changed');`)}`);
    await attempt(
      "Metadata API deploy",
      "POST",
      `/services/Soap/m/${API.slice(1)}`,
      `<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:met="http://soap.sforce.com/2006/04/metadata"><soapenv:Header><met:SessionHeader><met:sessionId>${token.accessToken}</met:sessionId></met:SessionHeader></soapenv:Header><soapenv:Body><met:deploy><met:ZipFile>UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==</met:ZipFile><met:DeployOptions><met:checkOnly>true</met:checkOnly></met:DeployOptions></met:deploy></soapenv:Body></soapenv:Envelope>`,
      { "Content-Type": "text/xml; charset=UTF-8", SOAPAction: '"deploy"' },
    );
  } finally {
    const [after] = await adminQuery<{ Name: string }>(`SELECT Name FROM Account WHERE Id = '${recordId}'`);
    const strays = await adminQuery<{ Id: string }>("SELECT Id FROM Account WHERE Name LIKE 'Barometer write test 000%' AND Name != 'Barometer write test 0001'");
    const tasks = await adminQuery<{ Id: string }>(`SELECT Id FROM Task WHERE WhatId = '${recordId}'`);
    const opps = await adminQuery<{ Id: string }>("SELECT Id FROM Opportunity WHERE Name = 'Barometer write test'");
    console.log(`\nAfter: the test Account ${after ? `still exists, named "${after.Name}"` : "is gone"}. Stray Accounts: ${strays.length}, Tasks: ${tasks.length}, Opportunities: ${opps.length}`);
    const unchanged = after?.Name === TEST_NAME && strays.length === 0 && tasks.length === 0 && opps.length === 0;
    await sf(["data", "delete", "record", "--sobject", "Account", "--record-id", recordId]);
    console.log(`Admin deleted test Account ${recordId}`);

    console.log("\n| Attempt | HTTP | Salesforce code | Message | Refused |\n| --- | --- | --- | --- | --- |");
    for (const a of attempts) console.log(`| ${a.name} | ${a.status} | ${a.code || "-"} | ${a.message.replaceAll("|", "/") || "-"} | ${a.refused ? "yes" : "NO"} |`);
    const passed = readOk && attempts.length === 9 && attempts.every((a) => a.refused) && unchanged;
    console.log(`\n${passed ? "PASS" : "FAIL"}: read ${readOk ? "worked" : "failed"}, ${attempts.filter((a) => a.refused).length} of ${attempts.length} writes refused, org ${unchanged ? "unchanged" : "CHANGED"}`);
    if (!passed) process.exitCode = 1;
  }
}

await main();
