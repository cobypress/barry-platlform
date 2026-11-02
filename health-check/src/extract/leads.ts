import { readCapped } from "./opportunities.ts";
import { readSoql, soqlDateTime, type Extractor, type Sourced } from "./types.ts";

// No names, emails, phone numbers or company names: a funnel is counts and dates.
export type LeadRow = {
  Id: string;
  OwnerId: string;
  LeadSource: string | null;
  CreatedDate: string;
  IsConverted: boolean;
  ConvertedDate: string | null;
  ConvertedOpportunityId: string | null;
  ConvertedContactId: string | null;
};
export type LeadStatusChange = { Id: string; LeadId: string; CreatedDate: string };
export type CampaignRow = { Id: string; Type: string | null; ActualCost: number | null };
export type CampaignMemberRow = { Id: string; CampaignId: string; LeadId: string | null };
export type LeadExtract = { leads: Sourced<LeadRow[]>; statusChanges: Sourced<LeadStatusChange[]>; campaigns: Sourced<CampaignRow[]>; members: Sourced<CampaignMemberRow[]> };

export const leadsExtractor: Extractor<LeadExtract> = {
  id: "leads",
  version: 1,
  partialKeys: ["leads", "lead-status", "campaign-members"],
  async run(ctx) {
    const since = `CreatedDate >= ${soqlDateTime(ctx.windowStart)}`;
    return {
      leads: await readCapped<LeadRow>(ctx, "leads", "Id, OwnerId, LeadSource, CreatedDate, IsConverted, ConvertedDate, ConvertedOpportunityId, ConvertedContactId", "Lead", since),
      // Only that a status changed, and when: the old and new values aren't needed.
      statusChanges: await readCapped<LeadStatusChange>(ctx, "lead-status", "Id, LeadId, CreatedDate", "LeadHistory", `Field = 'Status' AND ${since}`),
      // The campaign's Name can name a partner, and no rule needs it.
      campaigns: await readSoql<CampaignRow>(ctx, "SELECT Id, Type, ActualCost FROM Campaign"),
      members: await readCapped<CampaignMemberRow>(ctx, "campaign-members", "Id, CampaignId, LeadId", "CampaignMember", `LeadId != null AND ${since}`),
    };
  },
};
