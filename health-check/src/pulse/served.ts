import { digestText } from "../digest/build.ts";
import { spansIfCurrent } from "../engine/org-words.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { MASK } from "../render/repair.ts";
import type { RunDigest } from "../schemas/digest.ts";
import type { PulseTitle } from "./result.ts";

// An automatic customer's pulse result lists titles only as the checked digest
// serves them (DECISIONS.md 746). The result is a plain list the portal and the
// monthly email print as they stand, with no place for marks, so a title is
// listed only where every word of it is ours as served: the digest's own
// replacement for one naming people, or a title the engine wrote with no word
// of the org's. A title with the org's words, one the digest withheld or
// masked, or one whose marks are unknown is left out of the list and still
// counted. The digest serves each of those marked, in its own place.

/** The titles an automatic customer's pulse result may list, from its checked digest. */
export function servedPulseTitle(digest: RunDigest, pseudonyms: Pseudonymiser): PulseTitle {
  const served = new Map(digest.findings.map((f) => [f.id, f]));
  return ({ title, findingId, baselineFinding }) => {
    // A new issue is this run's, so its title is the digest's, as served.
    if (findingId !== undefined) {
      const d = served.get(findingId);
      if (!d || d.title.includes(MASK)) return null;
      const marks = d.orgWords?.title;
      return marks && marks.orgWords.length === 0 ? d.title : null;
    }
    // A fixed issue's title is the baseline's, which this run's data can't trace: a figure in it is left out whole.
    if (/\d/.test(title)) return null;
    const guarded = digestText(title, pseudonyms);
    if (guarded !== title) return guarded;
    const spans = baselineFinding && baselineFinding.title === title ? spansIfCurrent(title, baselineFinding.orgWords?.title) : null;
    return spans && spans.length === 0 ? title : null;
  };
}
