import { join } from "node:path";
import { automaticCalibration } from "../connect/auto-calibration.ts";
import type { OrgStage } from "../connect/org-stages.ts";
import { readCheckpoint } from "../engine/checkpoint.ts";
import type { StageRow } from "../extract/opportunities.ts";
import type { Calibration } from "../schemas/index.ts";
import type { Transport, TransportRequest, TransportResponse } from "../salesforce/transport.ts";
import { runFromRecording } from "./harness.ts";
import { loadRecording, ReplayTransport, requestKey, type Recording } from "./replay.ts";

// The synthetic org as an automatic run sees it (DECISIONS.md 693): its
// calibration built from the org's own stages, by the code the connect job
// runs, never the fixture's confirmed one. Made from the recording's stages
// read, so a change to the synthetic org's stages moves it too.

/** The org's stages as the connect job reads them, from a run's checkpoint. */
export function stagesOf(dir: string): OrgStage[] {
  const read = readCheckpoint<{ status: string; data?: StageRow[] }>(join(dir, "checkpoints", "opportunity-stages.json"));
  if (read.status !== "ok" || !read.data) throw new Error("The synthetic org's stages weren't read");
  return read.data.map((s) => ({ apiName: s.ApiName, label: s.MasterLabel, isActive: s.IsActive, isClosed: s.IsClosed, isWon: s.IsWon, sortOrder: s.SortOrder ?? null }));
}

let made: Calibration | undefined;

/** The synthetic org's automatic calibration, worked out once per process. */
export async function automaticSyntheticCalibration(): Promise<Calibration> {
  if (made) return made;
  const { dir, run } = await runFromRecording();
  const built = automaticCalibration(stagesOf(dir), { orgId: run.orgId, at: "2026-10-31T09:00:00.000Z", currency: "GBP" });
  if (!built.ok) throw new Error(built.reason);
  made = built.calibration;
  return made;
}

/**
 * The recording, answering an automatic run's reads. The recording was made with
 * the fixture's calibration, and two parts of the opportunities read follow the
 * calibration: the loss reason field, which only the fixture's names, and the
 * open stages the read takes whatever their dates, which the automatic one
 * takes from the org's flags. A read that differs only there is answered with
 * the recorded reply: the same records, perhaps a few more, and a field the run
 * doesn't read. The run then keeps what its own calibration says, as it would
 * from a real org. Every other request must be recorded exactly.
 */
export class AutomaticReplay implements Transport {
  readonly #inner: ReplayTransport;
  readonly #recorded = new Map<string, string>();

  constructor(recording: Recording = loadRecording()) {
    this.#inner = new ReplayTransport(recording);
    for (const { key } of recording.exchanges) this.#recorded.set(loose(key), key);
  }

  /** Every request sent, as the replay saw it. */
  get requests(): TransportRequest[] {
    return this.#inner.requests;
  }

  send(request: TransportRequest): Promise<TransportResponse> {
    const key = requestKey(request);
    const recorded = this.#recorded.get(loose(key));
    if (!recorded || recorded === key) return this.#inner.send(request);
    const url = new URL(request.url);
    return this.#inner.send({ ...request, url: `${url.origin}${recorded.slice(recorded.indexOf(" ") + 1)}` });
  }
}

/** A request's key without the parts of the opportunities read that follow the calibration. */
const loose = (key: string): string => key.replace("%2C%20Loss_Reason__c%20FROM%20Opportunity", "%20FROM%20Opportunity").replace(/%20OR%20StageName%20IN%20\([^)]*\)/, "");
