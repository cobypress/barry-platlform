import { pulseOfAutomaticCheck } from "./run-job.ts";
import type { WorkerStore } from "./types.ts";

// A person's Show of an automatic customer's held monthly scan (DECISIONS.md
// 746): what the console's "Show to customer" does for a held pulse. The pulse
// must be one whose baselines lead back to an automatic check of its own org,
// held only for checks a person may judge past (score_moved, reads_lost,
// mostly_unmeasured), with its would-be result kept; then that result is
// published as the pulse's, the flags are cleared by the person and an audit
// row says so, in one transaction (the store's showHeldPulse). Once shown, the
// next month's pulse is measured against it.

export async function showHeldPulse(store: WorkerStore, runId: string, by: string, at: string): Promise<{ ok: true; flagsCleared: string[] } | { ok: false; why: string }> {
  if (!/\S+@\S+/.test(by)) return { ok: false, why: "A Show names the person who chose it." };
  const self = await store.runLineage(runId);
  if (!self || self.product !== "pulse") return { ok: false, why: "Only an automatic customer's monthly scan is shown this way." };
  if (!(await pulseOfAutomaticCheck({ id: runId, orgId: self.orgId, baselineRunId: self.baselineRunId }, { store }))) return { ok: false, why: "Only an automatic customer's monthly scan is shown this way." };
  return store.showHeldPulse(runId, by, at);
}
