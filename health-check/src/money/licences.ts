// Which licence types cost money. Salesforce lists many free ones in every org,
// some with thousands of seats, so counting them as paid would swamp the figures.

// Salesforce includes these with the edition at no charge. Extra seats can be
// bought, so a calibration that prices one counts it as paid.
export const COMPLIMENTARY_LICENCES = new Set([
  "Salesforce Integration",
  "Cloud Integration User",
  "Analytics Cloud Integration User",
  "Sales Insights Integration User",
  "Salesforce Limited Access - Free",
  "Identity",
  "Chatter Free",
  "Chatter External",
  "Guest User License",
]);

/** A licence type worth counting: active, with seats, and either not free or priced by the calibration. */
export function isPaidLicence(licence: { Name: string; TotalLicenses: number; Status: string }, licenceCosts: Record<string, number>): boolean {
  if (licence.Status !== "Active" || licence.TotalLicenses <= 0) return false;
  return licenceCosts[licence.Name] !== undefined || !COMPLIMENTARY_LICENCES.has(licence.Name);
}
