import type { Interview } from "../interviews/questions.ts";

// Made-up stakeholder interviews for tests and the sample decks. Every answer
// and note is invented, ids are zero-padded, and nobody is named: the notes
// speak of roles, as the console asks consultants to write them.

/** A line in the notes that must never be printed in any document, so a test can look for it. */
export const SYNTHETIC_NOTE = "Synthetic interview note that must never be printed 4c1e";

/** Five conversations: a sales director, sales operations, the admin and two reps. */
export function syntheticInterviews(): Interview[] {
  const at = (n: number) => `ia-${String(n).padStart(10, "0")}`;
  return [
    {
      id: at(1),
      role: "sales_leader",
      heldOn: "2026-10-27",
      notes: `Doesn't rely on the pipeline report for the board: builds the forecast in a spreadsheet each Friday from calls with the managers. Thinks the win rate is around 40%. Says deals take about two months. ${SYNTHETIC_NOTE}`,
      answers: { trust: 2, win_rate: 40, forecast_hit: 70, sales_cycle: 60, fix_first: "One pipeline view everyone believes." },
      version: 1,
    },
    {
      id: at(2),
      role: "sales_ops",
      heldOn: "2026-10-28",
      notes: "Spends Monday mornings chasing reps to update close dates. Several stages were added over the years and nobody removed the old ones. Keeps a spreadsheet of renewals alongside Salesforce.",
      answers: { trust: 2, stages: 2, rekeying: 4, integrations: 3, fix_first: "Fewer stages, with clear exit criteria." },
      version: 1,
    },
    {
      id: at(3),
      role: "admin",
      heldOn: "2026-10-28",
      notes: "Took over from a contractor last year. Worried about touching the older automation because nobody documented it. Says about 30 people log in most weeks.",
      answers: { ease: 4, automation: 2, weekly_users: 30, integrations: 4, fix_first: "Documentation for what fires when a deal is saved." },
      version: 1,
    },
    {
      id: at(4),
      role: "sales_rep",
      heldOn: "2026-10-29",
      notes: "Updates opportunities at the end of the week in one go. Logs calls in another tool, not Salesforce. Finds the opportunity page has too many fields.",
      answers: { ease: 2, stages: 3, rekeying: 2, admin_hours: 3, fix_first: "Fewer required fields." },
      version: 1,
    },
    {
      id: at(5),
      role: "sales_rep",
      heldOn: "2026-10-30",
      notes: "Newer to the team. Fine with Salesforce on the whole, but copies notes from email by hand.",
      answers: { ease: 4, stages: 4, rekeying: 1.5, admin_hours: 2 },
      version: 1,
    },
  ];
}
