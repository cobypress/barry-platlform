import { leadsExtractor } from "../extract/leads.ts";
import { opportunitiesExtractor } from "../extract/opportunities.ts";
import { AS_OF, sfId, userId, type SyntheticHistory, type SyntheticLead, type SyntheticOpportunity } from "./synthetic-org.ts";
import { SYNTHETIC_RECORDS, type OrgRecords } from "./simulator.ts";

// Constructed lead sets for the lead journey's tests (DECISIONS.md 770): leads,
// the deals they convert into and those deals' stage history, put beside the
// synthetic org's own deals, so a test sets every lead's day, conversion and
// outcome. No real data: ids are zero-padded from 900,000, so none meets the
// synthetic org's.

const DAY = 86_400_000;
const NOW = Date.parse(AS_OF);
const FIRST = 900_000;

/** One lead: its source, how many days before the run it was made, and what became of it. */
export type LeadSpec = {
  source: string | null;
  /** Days before the run the lead was made, at noon UTC unless `at` says another time of day. */
  made: number;
  /** "HH:MM" UTC on that day. */
  at?: string;
  /** Days after it was made that it converted; absent, it never did. */
  convertedAfter?: number;
  /** What its deal did: absent with a conversion, it converted with no deal. */
  outcome?: "won" | "lost" | "open";
  /** Days after the lead was made that its deal closed. */
  closedAfter?: number;
  /** Synthetic user 1 to 3, whose roles are the teams. */
  owner?: number;
  /** Another lead's deal, by index, for a deal converted from two leads. */
  sameDealAs?: number;
};

const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
const dayOf = (ms: number): string => new Date(ms).toISOString().slice(0, 10);

export type JourneyRecords = { leads: SyntheticLead[]; statusChanges: Array<{ Id: string; LeadId: string; CreatedDate: string }>; opportunities: SyntheticOpportunity[]; history: SyntheticHistory[] };

/**
 * The leads, their deals and history. By default each lead gets one Status
 * change an hour after it was made, so the history reads as kept; `status`
 * replaces them.
 */
export function journeyRecords(specs: LeadSpec[], status?: (leads: SyntheticLead[]) => JourneyRecords["statusChanges"]): JourneyRecords {
  const leads: SyntheticLead[] = [];
  const opportunities: SyntheticOpportunity[] = [];
  const history: SyntheticHistory[] = [];
  const dealOf: Array<string | null> = [];
  specs.forEach((s, i) => {
    const n = FIRST + i;
    const madeMs = Date.parse(`${dayOf(NOW - s.made * DAY)}T${s.at ?? "12:00"}:00.000Z`);
    const converted = s.convertedAfter !== undefined;
    let deal: string | null = null;
    if (converted && s.sameDealAs !== undefined) deal = dealOf[s.sameDealAs] ?? null;
    else if (converted && s.outcome) {
      deal = sfId("006", n);
      const openedMs = madeMs + s.convertedAfter! * DAY;
      const closeMs = s.outcome === "open" ? NOW + 30 * DAY : Date.parse(`${dayOf(madeMs + (s.closedAfter ?? s.convertedAfter! + 30) * DAY)}T10:00:00.000Z`);
      const stage = s.outcome === "won" ? "Closed Won" : s.outcome === "lost" ? "Closed Lost" : "Proposal";
      opportunities.push({
        Id: deal, OwnerId: userId(s.owner ?? 1), StageName: stage, Amount: 1000, CloseDate: dayOf(closeMs), CreatedDate: stamp(openedMs),
        IsClosed: s.outcome !== "open", IsWon: s.outcome === "won", LastActivityDate: null, Type: null, LeadSource: s.source, RecordType: { DeveloperName: "Standard" }, Loss_Reason__c: s.outcome === "lost" ? "Price" : null, AccountId: null, NextStep: null,
      });
      history.push({ Id: sfId("008", n * 2), OpportunityId: deal, StageName: "Prospecting", Amount: 1000, CloseDate: dayOf(closeMs), CreatedDate: stamp(openedMs) });
      if (s.outcome !== "open") history.push({ Id: sfId("008", n * 2 + 1), OpportunityId: deal, StageName: stage, Amount: 1000, CloseDate: dayOf(closeMs), CreatedDate: stamp(closeMs) });
    }
    dealOf.push(deal);
    leads.push({
      Id: sfId("00Q", n), OwnerId: userId(s.owner ?? 1), LeadSource: s.source, CreatedDate: stamp(madeMs), IsConverted: converted,
      ConvertedDate: converted ? dayOf(madeMs + s.convertedAfter! * DAY) : null, ConvertedOpportunityId: deal, ConvertedContactId: converted ? sfId("003", n) : null,
    });
  });
  const statusChanges = status ? status(leads) : leads.map((l, i) => ({ Id: sfId("017", FIRST + i), LeadId: l.Id, CreatedDate: stamp(Date.parse(l.CreatedDate.replace("+0000", "Z")) + 3_600_000) }));
  return { leads, statusChanges, opportunities, history };
}

/** The synthetic org's records with these leads in place of its own, and their deals beside its own: for the simulator. */
export function journeyOrg(r: JourneyRecords): OrgRecords {
  return {
    ...SYNTHETIC_RECORDS,
    pipeline: { opportunities: [...SYNTHETIC_RECORDS.pipeline.opportunities, ...r.opportunities], history: [...SYNTHETIC_RECORDS.pipeline.history, ...r.history].sort((a, b) => a.Id.localeCompare(b.Id)) },
    leadFunnel: { leads: r.leads, statusChanges: r.statusChanges, members: [] },
  };
}

type Editable = { leads: { data: unknown[] }; statusChanges: { data: unknown[] }; members: { data: unknown[] } };
type Opps = { opportunities: { data: unknown[] }; history: { data: unknown[] } };

/** A run's checkpoints with these leads in place of its own and their deals beside its own: for analyseExtracts. */
export function withJourney(extracts: Map<string, unknown>, r: JourneyRecords): Map<string, unknown> {
  const changed = structuredClone(extracts);
  const leads = changed.get(leadsExtractor.id) as Editable;
  leads.leads.data = r.leads;
  leads.statusChanges.data = r.statusChanges;
  leads.members.data = [];
  const opps = changed.get(opportunitiesExtractor.id) as Opps;
  opps.opportunities.data = [...opps.opportunities.data, ...r.opportunities.map(({ RecordType, Loss_Reason__c, NextStep: _n, ...o }) => ({ ...o, RecordType: RecordType.DeveloperName, CurrencyIsoCode: null, LossReason: Loss_Reason__c }))];
  opps.history.data = [...opps.history.data, ...r.history];
  return changed;
}

/**
 * A rich org: three sources of 200 leads made 100 to 699 days before the run.
 * Webinar converts half its leads and wins 1 deal in 5; Web and Event convert 3
 * in 10 and win 6 in 10. Half of each source's deals converted within 3 days,
 * half after 20, and winning doesn't follow which. Owners 1 to 3 in turn.
 */
export function richSpecs(): LeadSpec[] {
  const specs: LeadSpec[] = [];
  for (const [source, converts, wins] of [["Webinar", (k: number) => k % 2 === 0, (j: number) => j % 5 === 0], ["Web", (k: number) => k % 10 < 3, (j: number) => j % 10 < 6], ["Event", (k: number) => k % 10 < 3, (j: number) => j % 10 < 6]] as const) {
    let j = 0;
    for (let k = 0; k < 200; k++) {
      const made = 100 + ((k * 7) % 600);
      const owner = (k % 3) + 1;
      if (!converts(k)) {
        specs.push({ source, made, owner });
        continue;
      }
      const convertedAfter = j % 2 === 0 ? 3 : 20;
      specs.push({ source, made, owner, convertedAfter, outcome: wins(j) ? "won" : "lost", closedAfter: convertedAfter + 30 + (j % 4) * 10 });
      j++;
    }
  }
  return specs;
}
