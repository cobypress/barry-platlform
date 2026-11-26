import { ORG_SETTINGS_CALIBRATOR } from "../connect/auto-calibration.ts";
import { StoreError, type OrgStage } from "../connect/org-stages.ts";
import { fromModel, fromName, machineSet, withMachineStages } from "../connect/stage-outcomes.ts";
import { Calibration } from "../schemas/calibration.ts";
import { answeredStages } from "./connect-job.ts";
import type { WorkerDeps } from "./deps.ts";
import { CalibrationUnsettled } from "./name-currency.ts";
import type { RunJobInput } from "./types.ts";
import { modelStages, stageModelOn } from "./stage-model.ts";

// A saved automatic calibration's closed stages, set again when its run starts
// afresh (DECISIONS.md 756): the customer's newest answer to which are lost and
// which no decision, and, with HC_STAGE_NAMES on, the name rule for those the
// answer leaves out. A calibration built at connect or by Run again took both
// when it was built; one saved before the customer answered, or before the
// switch changed, takes them here, before the run's checks use it. Nothing
// reads Salesforce: the stages are the ones connect last saved for the run.
// Only a calibration the machine built is touched, and in it only the stages
// it set itself: never a person's calibration, never a stage a person or
// Salesforce named. Stage names never reach the log: counts only.

/** What the worker says at start about the stage-name rule. */
export const stageNamesStartLine = (on: boolean): string =>
  on
    ? "Closed stages read by name are on: an automatic run reads a closed stage that isn't won as lost or no decision from its name where the customer hasn't said, and says so as an assumption."
    : "Closed stages read by name are off: HC_STAGE_NAMES isn't on, so a closed stage that isn't won stays undecided until the customer says which it is.";

/** What the worker says at start about a model reading the stages the rule leaves undecided (DECISIONS.md 766). */
export const stageModelStartLine = (names: boolean, on: boolean, claude: boolean, consoleReady: boolean): string =>
  !on
    ? "Closed stages read by a model are off: HC_STAGE_NAMES_MODEL isn't on, so no stage name is sent to a model."
    : !consoleReady
      ? "Closed stages read by a model: HC_STAGE_NAMES_MODEL is on but ignored: the console doesn't carry model readings yet (HC_STAGE_NAMES_MODEL_CONSOLE isn't ready), so no stage name is sent to a model."
      : !names
      ? "Closed stages read by a model are switched on, but HC_STAGE_NAMES isn't, so no stage name is sent to a model."
      : claude
        ? "Closed stages read by a model are on: an automatic calibration sends the label and API name of each closed stage that isn't won, that the word list leaves undecided and whose every word is on the fixed vocabulary, one stage a call and nothing else, to Claude, and says its reading as an assumption."
        : "Closed stages read by a model are switched on, but Claude isn't set up, so no stage name is sent and those stages stay undecided.";

/**
 * The calibration the run goes on with. The caller has checked that this
 * delivery starts the run afresh (no `run.json`). Throws only
 * CalibrationUnsettled, and only on a delivery that isn't the last: after a
 * write that threw or matched nothing, the run goes on with the row as stored,
 * as 745's naming does, so a resumed delivery never finds its calibration changed.
 */
export async function setStagesBeforeRun(o: { runId: string; input: RunJobInput; deps: WorkerDeps; log: (line: string) => void; final?: boolean }): Promise<RunJobInput["calibration"]> {
  const { runId, input, deps, log } = o;
  const c = input.calibration;
  if (!c || input.run.reviewMode !== "automatic") return c;
  // Built by the machine and confirmed by nobody: a person's calibration is theirs (a person's save makes the source "confirmed").
  if (c.source !== "org_settings" || c.confirmedBy !== ORG_SETTINGS_CALIBRATOR) return c;
  if (c.orgId && c.orgId.slice(0, 15) !== input.run.orgId.slice(0, 15)) return c;
  const byName = deps.stageNames === true;
  const answer = await answeredStages(runId, input.run.orgId, deps, log);
  // Nothing to set and nothing set before: the calibration is as it was, and nothing more is read.
  if (!answer && !byName && !c.stages.some(machineSet)) return c;
  let org: OrgStage[] | null;
  try {
    org = await deps.store.savedOrgStages(runId);
  } catch (err) {
    log(`Automatic run ${runId}'s saved stages couldn't be read, so its closed stages are left as its calibration has them: ${err instanceof StoreError ? err.message : "an unexpected error"}.`);
    return c;
  }
  // With no saved stages nothing new can be checked, so nothing is added; with the switch off, readings from names still go (the analysis sets them aside anyway).
  const byModel = stageModelOn(deps) && input.run.product === "barometer";
  const set = org ? withMachineStages(c, org, answer, byName) : withoutNames(c, byName, byModel);
  if (!org) log(`Automatic run ${runId} has no saved stages to check its closed stages against, so ${set.changed ? "only the readings from stage names were taken out" : "they are left as its calibration has them"}.`);
  // Below every other word, the model reads only what is still undecided, reusing what it read before while the stage's label is unchanged (DECISIONS.md 766).
  const model = org && byModel ? await modelStages({ runId, org, decided: new Set(set.stages.map((s) => s.name)), previous: c.stages, previousUnsure: c.modelUnsure, client: deps.narrative, log }) : null;
  const read = model?.stages ?? [];
  const stages = read.length > 0 ? [...set.stages, ...read] : set.stages;
  // The model's "unsure" answers ride beside the stages; with the route off, any kept before go with its readings.
  const unsure = model ? model.unsure : c.modelUnsure?.length ? [] : undefined;
  const unsureChanged = unsure !== undefined && JSON.stringify(unsure) !== JSON.stringify(c.modelUnsure ?? []);
  const changed = (read.length > 0 ? JSON.stringify(stages) !== JSON.stringify(c.stages) : set.changed) || unsureChanged;
  if (!changed) return c;
  const { modelUnsure: _kept, ...rest } = c;
  const parsed = Calibration.safeParse({ ...(unsure === undefined ? c : rest), stages, ...(unsure?.length ? { modelUnsure: unsure } : {}) });
  if (!parsed.success) {
    log(`Automatic run ${runId}'s calibration refused its closed stages as set again, so they are left as they were.`);
    return c;
  }
  let saved: "updated" | "unchanged";
  try {
    saved = unsure === undefined ? await deps.store.answerCalibrationStages(runId, c.stages, parsed.data.stages) : await deps.store.answerCalibrationStages(runId, c.stages, parsed.data.stages, unsure);
  } catch (err) {
    // The write may have been committed before the error reached us: the row decides.
    log(`Automatic run ${runId}'s closed stages may not have been saved (${err instanceof StoreError ? err.message : "an unexpected error"}), so the run goes on with the calibration as stored.`);
    return asStored(o, c);
  }
  if (saved === "unchanged") {
    log(`Automatic run ${runId}'s calibration changed while its closed stages were set, so the run goes on with the one saved now.`);
    return asStored(o, c);
  }
  log(`Automatic run ${runId}'s closed stages were set again before it ran: ${set.answered} from the customer's answer, ${set.byName} from their names${byModel ? `, ${read.length} read by a model` : ""}${set.ignored > 0 ? `, ${set.ignored} ${set.ignored === 1 ? "entry" : "entries"} in the answer ignored` : ""}; nothing else in its calibration changed.`);
  return parsed.data;
}

/** The stages without what names read, with the switch off, and without what a model read, with its switch off; with them on, as they are. Counts as withMachineStages gives them. */
function withoutNames(c: Calibration, byName: boolean, byModel: boolean): ReturnType<typeof withMachineStages> {
  const stages = c.stages.filter((s) => (byName || !fromName(s)) && (byModel || !fromModel(s)));
  return { stages, answered: 0, byName: 0, ignored: 0, changed: stages.length !== c.stages.length };
}

/** The calibration as the store holds it now; unreadable, a delivery the queue will make again stops before the run starts. */
async function asStored(o: { runId: string; deps: WorkerDeps; log: (line: string) => void; final?: boolean }, c: Calibration): Promise<Calibration> {
  const now = await o.deps.store.loadRunJob(o.runId).catch(() => null);
  if (now?.calibration) return now.calibration;
  if (!o.final) throw new CalibrationUnsettled(`Automatic run ${o.runId}'s calibration couldn't be read again after its closed stages were saved, so this delivery stops before the run starts and the queue delivers it again.`);
  o.log(`Automatic run ${o.runId}'s calibration couldn't be read again after its closed stages were saved, and this is the last delivery, so the run goes on with the calibration it had.`);
  return c;
}
