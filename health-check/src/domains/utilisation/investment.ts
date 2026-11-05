import { orgExtractor } from "../../extract/configuration.ts";
import { opportunitiesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
import { licencesExtractor, usersExtractor, type UserLicenseRow } from "../../extract/users.ts";
import { pct, plural, type RuleContext } from "../../engine/rule.ts";
import { isPaidLicence } from "../../money/licences.ts";
import { costPerSeat, isPriced, type SeatPrice, type SeatPricing } from "../../money/seat-cost.ts";
import type { Evidence, Metric, MetricSegment } from "../../schemas/index.ts";

// Roadmap 1.3: what the customer pays for Salesforce and what they use. Seats
// come from the org's own licence counts. Costs are seats times a price per seat
// from the calibration, then list price, and say which. What the customer told
// us they spend in total isn't known here: it's on the intake, which the report
// reads (src/money/investment.ts).

type Ctx = Omit<RuleContext, "metrics">;

export const INVESTMENT = {
  seatsPaid: "investment.seats_paid",
  seatsUsed: "investment.seats_used",
  seatsUnused: "investment.seats_unused",
  seatCost: "investment.seat_cost",
  unusedSeatCost: "investment.unused_seat_cost",
  listPriceCost: "investment.list_price_cost",
  activeUserShare: "investment.active_user_share",
  packageSeatsPaid: "investment.package_seats_paid",
  packageSeatsUnused: "investment.package_seats_unused",
} as const;

export const INVESTMENT_METRIC_IDS: string[] = Object.values(INVESTMENT);

const DOMAIN = "investment";
const LICENCE_EXCLUSION = "Free licence types, such as Chatter Free and the integration licences that come with the edition, unless the calibration gives them a price";

type Definition = Pick<Metric, "id" | "label" | "unit" | "definition">;
const DEFINITIONS: Record<keyof typeof INVESTMENT, Definition> = {
  seatsPaid: { id: INVESTMENT.seatsPaid, label: "Seats paid for", unit: "count", definition: "Salesforce user licences the org holds, by licence type. Free licence types are left out." },
  seatsUsed: { id: INVESTMENT.seatsUsed, label: "Seats in use", unit: "count", definition: "Of the seats paid for, those assigned to a user, by licence type." },
  seatsUnused: { id: INVESTMENT.seatsUnused, label: "Seats unused", unit: "count", definition: "Seats paid for that no user holds, by licence type." },
  seatCost: { id: INVESTMENT.seatCost, label: "Annual cost of seats", unit: "currency", definition: "Seats paid for times a year's cost per seat: the calibration's figure, or Salesforce's list price where it has none." },
  unusedSeatCost: { id: INVESTMENT.unusedSeatCost, label: "Annual cost of unused seats", unit: "currency", definition: "Unused seats times a year's cost per seat, priced as the seats are." },
  listPriceCost: { id: INVESTMENT.listPriceCost, label: "Seats at list price", unit: "currency", definition: "Seats paid for at Salesforce's published list price, for context. It isn't what you pay." },
  activeUserShare: { id: INVESTMENT.activeUserShare, label: "Seats in active use", unit: "percent", definition: "Users on a paid licence who logged in within the dormant period, as a share of seats in use." },
  packageSeatsPaid: { id: INVESTMENT.packageSeatsPaid, label: "Package seats paid for", unit: "count", definition: "Seats on installed managed packages that sell seats, by namespace. Free, trial, expired and site-wide licences are left out." },
  packageSeatsUnused: { id: INVESTMENT.packageSeatsUnused, label: "Package seats unused", unit: "count", definition: "Package seats paid for that no user holds, by namespace." },
};

type Measured = Extract<Metric, { status: "measured" }>;

const measured = (d: Definition, evidence: Evidence[], value: number, n: number, segments: MetricSegment[], extra: Partial<Measured> = {}): Metric => ({
  ...d,
  domain: DOMAIN,
  status: "measured",
  evidence,
  exclusions: extra.exclusions ?? [LICENCE_EXCLUSION],
  assumptions: extra.assumptions ?? [],
  value,
  n,
  dateRange: null,
  segments,
  segmentExemptReason: segments.length ? null : "There are no seats to split by licence.",
  series: [],
  stats: extra.stats ?? {},
});

const notMeasurable = (d: Definition, evidence: Evidence[], reason: string): Metric => ({ ...d, domain: DOMAIN, status: "not_measurable", reason, evidence, exclusions: [], assumptions: [] });

const segment = (key: string, n: number, value: number | null, reason: string | null = null): MetricSegment => ({ dimension: "licence", key, n, value, notMeasurableReason: reason });
const unused = (l: { TotalLicenses: number; UsedLicenses: number }): number => Math.max(0, l.TotalLicenses - l.UsedLicenses);

/** Seats times a price per seat, measured only when every licence with seats has a price: a part-priced total would understate. */
function costMetric(d: Definition, evidence: Evidence[], licences: UserLicenseRow[], seatsOf: (l: UserLicenseRow) => number, price: (licence: string) => ReturnType<typeof costPerSeat>, describe: string): Metric {
  const counted = licences.filter((l) => seatsOf(l) > 0);
  const prices = counted.map((l) => ({ licence: l, price: price(l.Name) }));
  const missing = prices.filter((p) => !isPriced(p.price));
  if (missing.length) return notMeasurable(d, evidence, `${describe} can't be priced, because ${missing.map((p) => `${p.licence.Name} has no cost per seat (${"reason" in p.price ? p.price.reason : ""})`).join("; ")}.`);
  const priced = prices.map((p) => ({ licence: p.licence, price: p.price as SeatPrice, seats: seatsOf(p.licence) }));
  const value = priced.reduce((sum, p) => sum + p.seats * p.price.annual, 0);
  const seats = priced.reduce((sum, p) => sum + p.seats, 0);
  return measured(
    d,
    evidence,
    value,
    seats,
    priced.map((p) => segment(p.licence.Name, p.seats, p.seats * p.price.annual)),
    {
      assumptions: priced.map((p) => `${plural(p.seats, `${p.licence.Name} seat`)} at ${p.price.note}`),
      // Which prices were used, so a report can label the total without reading the text.
      stats: { calibrationPriced: priced.filter((p) => p.price.basis === "calibration").length, listPriced: priced.filter((p) => p.price.basis === "list_price").length },
    },
  );
}

export function investmentMetrics(ctx: Ctx): Metric[] {
  const { userLicenses, packageLicenses } = ctx.data(licencesExtractor);
  const users = ctx.data(usersExtractor);
  const org = ctx.data(orgExtractor);
  const licenceEvidence = [ctx.ev(userLicenses.source, { count: userLicenses.status === "ok" ? userLicenses.data.length : null })];
  const packageEvidence = [ctx.ev(packageLicenses.source, { count: packageLicenses.status === "ok" ? packageLicenses.data.length : null })];
  const all = Object.values(DEFINITIONS);
  // DECISIONS 37: no metric is published when Opportunity can't be read.
  const { opportunities } = ctx.data(opportunitiesExtractor);
  if (opportunities.status !== "ok") return all.map((d) => notMeasurable(d, d.id.startsWith("investment.package") ? packageEvidence : licenceEvidence, `Opportunities couldn't be read, and no metric is published without them: ${opportunities.reason}`));

  const metrics: Metric[] = [];
  if (userLicenses.status !== "ok") {
    const reason = `Licences couldn't be read: ${userLicenses.reason}`;
    for (const d of [DEFINITIONS.seatsPaid, DEFINITIONS.seatsUsed, DEFINITIONS.seatsUnused, DEFINITIONS.seatCost, DEFINITIONS.unusedSeatCost, DEFINITIONS.listPriceCost, DEFINITIONS.activeUserShare]) metrics.push(notMeasurable(d, licenceEvidence, reason));
  } else {
    const paid = userLicenses.data.filter((l) => isPaidLicence(l, ctx.calibration.licenceCosts)).sort((a, b) => b.TotalLicenses - a.TotalLicenses || a.Name.localeCompare(b.Name));
    const sum = (f: (l: UserLicenseRow) => number): number => paid.reduce((total, l) => total + f(l), 0);
    const totalSeats = sum((l) => l.TotalLicenses);
    const usedSeats = sum((l) => Math.min(l.UsedLicenses, l.TotalLicenses));
    metrics.push(
      measured(DEFINITIONS.seatsPaid, licenceEvidence, totalSeats, paid.length, paid.map((l) => segment(l.Name, l.TotalLicenses, l.TotalLicenses))),
      measured(DEFINITIONS.seatsUsed, licenceEvidence, usedSeats, paid.length, paid.map((l) => segment(l.Name, l.TotalLicenses, Math.min(l.UsedLicenses, l.TotalLicenses)))),
      measured(DEFINITIONS.seatsUnused, licenceEvidence, totalSeats - usedSeats, paid.length, paid.map((l) => segment(l.Name, l.TotalLicenses, unused(l)))),
    );

    const edition = org.status === "ok" ? (org.data[0]?.OrganizationType ?? null) : null;
    const pricing: SeatPricing = { calibration: ctx.calibration, edition };
    const costEvidence = [...licenceEvidence, ctx.ev(org.source, { count: org.status === "ok" ? org.data.length : null, note: edition ? `Edition: ${edition}` : "Edition not read" })];
    metrics.push(
      costMetric(DEFINITIONS.seatCost, costEvidence, paid, (l) => l.TotalLicenses, (licence) => costPerSeat(licence, pricing), "The seats"),
      costMetric(DEFINITIONS.unusedSeatCost, costEvidence, paid, unused, (licence) => costPerSeat(licence, pricing), "The unused seats"),
      // List price alone, for context beside what they pay: the calibration is left out on purpose.
      costMetric(DEFINITIONS.listPriceCost, costEvidence, paid, (l) => l.TotalLicenses, (licence) => costPerSeat(licence, { ...pricing, calibration: { ...ctx.calibration, licenceCosts: {} } }), "At list price, the seats"),
    );

    metrics.push(activeShare(ctx, paid, usedSeats, licenceEvidence));
  }

  if (packageLicenses.status !== "ok") {
    const reason = `Installed packages' licences couldn't be read: ${packageLicenses.reason}`;
    metrics.push(notMeasurable(DEFINITIONS.packageSeatsPaid, packageEvidence, reason), notMeasurable(DEFINITIONS.packageSeatsUnused, packageEvidence, reason));
  } else {
    // AllowedLicenses is "the number of users allowed to use the package". A site
    // licence has no seat count we can use, so only positive counts are seats.
    const sold = packageLicenses.data.filter((p) => p.Status === "Active" && p.AllowedLicenses > 0).sort((a, b) => b.AllowedLicenses - a.AllowedLicenses);
    const name = (p: { NamespacePrefix: string | null }): string => p.NamespacePrefix ?? "No namespace";
    const exclusions = ["Packages that are free, on trial or expired", "Packages licensed to the whole org rather than by seat"];
    const seatsOf = (p: { AllowedLicenses: number; UsedLicenses: number }) => ({ TotalLicenses: p.AllowedLicenses, UsedLicenses: p.UsedLicenses });
    const total = sold.reduce((s, p) => s + p.AllowedLicenses, 0);
    const spare = sold.reduce((s, p) => s + unused(seatsOf(p)), 0);
    const packageSegment = (p: (typeof sold)[number], value: number): MetricSegment => ({ dimension: "package", key: name(p), n: p.AllowedLicenses, value, notMeasurableReason: null });
    metrics.push(
      measured(DEFINITIONS.packageSeatsPaid, packageEvidence, total, sold.length, sold.map((p) => packageSegment(p, p.AllowedLicenses)), { exclusions, stats: { packagesRead: packageLicenses.data.length } }),
      measured(DEFINITIONS.packageSeatsUnused, packageEvidence, spare, sold.length, sold.map((p) => packageSegment(p, unused(seatsOf(p)))), { exclusions, assumptions: ["Package seats aren't priced: Salesforce doesn't sell them, so there's no list price, and the calibration prices user licences"] }),
    );
  }
  return metrics;
}

function activeShare(ctx: Ctx, paid: UserLicenseRow[], usedSeats: number, licenceEvidence: Evidence[]): Metric {
  const d = DEFINITIONS.activeUserShare;
  const users = ctx.data(usersExtractor);
  const evidence = [...licenceEvidence, ctx.ev(users.source, { count: users.status === "ok" ? users.data.length : null })];
  if (users.status !== "ok") return notMeasurable(d, evidence, `Users couldn't be read: ${users.reason}`);
  if (usedSeats === 0) return notMeasurable(d, evidence, "No paid seat is in use, so there's no share to work out.");
  const days = ctx.calibration.dormantDays;
  const cutoff = daysBefore(ctx.now, days);
  const names = new Set(paid.map((l) => l.Name));
  const licenceOf = (u: (typeof users.data)[number]): string => u.Profile?.UserLicense?.Name ?? "Unknown";
  const recent = users.data.filter((u) => u.IsActive && names.has(licenceOf(u)) && u.LastLoginDate !== null && new Date(u.LastLoginDate) >= cutoff);
  // Seats in use come from Salesforce's licence counts, and users from the user
  // list. If they disagree this far, a share of one over the other means nothing.
  if (recent.length > usedSeats) return notMeasurable(d, evidence, `${recent.length} users logged in recently, more than the ${usedSeats} seats Salesforce counts as in use, so the two counts don't agree.`);
  const segments = paid
    .filter((l) => l.UsedLicenses > 0)
    .map((l) => {
      const inUse = Math.min(l.UsedLicenses, l.TotalLicenses);
      const active = recent.filter((u) => licenceOf(u) === l.Name).length;
      return active > inUse ? segment(l.Name, inUse, null, "More users logged in than seats in use") : segment(l.Name, inUse, pct(active, inUse));
    });
  return measured(d, evidence, pct(recent.length, usedSeats), usedSeats, segments, {
    exclusions: [LICENCE_EXCLUSION, "Inactive users"],
    assumptions: [`A user is active if they logged in within ${days} days, the calibration's dormant period, going by their last login date`],
    stats: { activeUsers: recent.length, dormantDays: days },
  });
}
