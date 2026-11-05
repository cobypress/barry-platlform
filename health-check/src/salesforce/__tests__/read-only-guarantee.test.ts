import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import type { TransportRequest, TransportResponse } from "../transport.ts";
import { ALLOWED_OPERATIONS, authorise } from "../allowlist.ts";

// Since anyone may sign in (DECISIONS 151), Barometer's only promise not to
// change an org is the request list in allowlist.ts. These hold the client to
// it, and run a whole health check as a user who could change anything.

describe("the client", () => {
  const source = readFileSync(fileURLToPath(new URL("../client.ts", import.meta.url)), "utf8");

  test("sends to Salesforce in exactly one place, straight after the allowlist", () => {
    assert.equal(source.match(/#transport\.send\(/g)?.length, 1);
    const send = source.slice(source.indexOf("async #send("));
    assert.ok(send.indexOf("authorise(request);") < send.indexOf("#transport.send("), "the check comes first");
  });

  test("has no way to send a request of the caller's choosing (rule 6)", () => {
    assert.doesNotMatch(source, /^\s*(?:async\s+)?(?:request|send|fetch|raw|call)\s*\(/m);
  });
});

/** An org that would let its user change anything: it answers like the synthetic org, and records anything that isn't a read. */
class AdminOrg extends Simulator {
  readonly writes: TransportRequest[] = [];
  override async send(request: TransportRequest): Promise<TransportResponse> {
    const read = request.method === "GET" || (request.method === "POST" && (/\/composite\/batch$/.test(new URL(request.url).pathname) || /\/services\/Soap\/m\//.test(request.url)));
    if (!read) this.writes.push(request);
    return super.send(request);
  }
}

describe("a health check as a full admin", () => {
  const org = new AdminOrg({ readOnly: false });
  let findings = 0;
  before(async () => {
    const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: org, log: () => {} });
    findings = JSON.parse(readFileSync(`${result.dir}/findings.json`, "utf8")).length;
  });

  test("runs every check end to end", () => {
    assert.ok(findings > 50, `${findings} findings`);
    assert.ok(org.requests.length > 20);
  });

  test("sends only requests the allowlist passes, and nothing that writes", () => {
    assert.deepEqual(org.writes, []);
    const used = new Set<string>();
    for (const request of org.requests) {
      const url = new URL(request.url);
      const id = authorise({ method: request.method, path: `${url.pathname}${url.search}`, headers: request.headers, ...(request.body === undefined ? {} : { body: request.body }) });
      assert.ok(ALLOWED_OPERATIONS.includes(id), id);
      used.add(id);
    }
    // The admin's route uses the Metadata API, which a read-only user can't.
    assert.ok(used.has("metadata.retrieve") && used.has("rest.query") && used.has("tooling.query"), [...used].join(", "));
    assert.ok(org.requests.every((r) => r.method === "GET" || r.method === "POST"));
  });
});
