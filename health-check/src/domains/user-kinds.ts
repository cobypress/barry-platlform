import { said, type Said } from "../engine/org-words.ts";
import type { RuleContext } from "../engine/rule.ts";
import type { Calibration } from "../schemas/index.ts";

// Which users are people (DECISIONS.md 693). A consultant's calibration names
// the shared, system, integration and test accounts; an automatic run's names
// none, since the org has no setting that says so and a guess from a name or a
// profile would be an estimate (rule 5). The rules then count every active user
// as a person, apart from those on integration licences, which they already
// leave out, and the checks that lean on that say so, through the engine's own
// assumption, as the unconfirmed stage order does (692).

/** Made in each analysis, as every said is. */
export const noUserKindsNote = (): Said => said`Nobody has told us which of your users are integrations, shared or system accounts, so we've counted every active user as a person, apart from those on integration licences.`;

/** The calibration's user kinds, saying so on the current rule or metric when an automatic run's calibration has none. */
export function userKinds(ctx: Pick<RuleContext, "calibration" | "assume">): Calibration["users"] {
  if (ctx.calibration.source === "org_settings" && ctx.calibration.users.length === 0) ctx.assume?.(noUserKindsNote());
  return ctx.calibration.users;
}
