// Builds the Salesforce payload for a completed funnel submission.
// Pure (no imports), so it's unit-tested with `node --test`.

export interface QuestionDef {
  key: string;
  text: string;
  type: string;
  /** Scoring area; absent/null for qualifiers (CRM, role…). */
  dimensionKey?: string | null;
  options?: Array<{ label: string; value: string }>;
}

/** Where a public step lives: its clean path, or /f/<slug>/<segment>. */
export interface StepPath {
  pathSegment: string;
  cleanPath: string | null;
}

export interface SubmissionForSync {
  id: string;
  /** The submission's private token: results and booking links carry it as ?s=. */
  token: string;
  funnelName: string;
  funnelSlug: string;
  funnelLive: boolean;
  sfCampaignId: string | null;
  sfLeadSource: string;
  questions: QuestionDef[];
  answers: Record<string, unknown>;
  scoreOverall: number | null;
  resultSnapshot: unknown;
  firstName: string | null;
  lastName: string | null;
  email: string | null;
  company: string | null;
  role: string | null;
  phone: string | null;
  companySize?: string | null;
  isFreeEmail?: boolean;
  utm: Record<string, unknown> | null;
  completedAt: Date | string | null;
  /** FunnelSubmission.optIns: the boxes ticked, [{ subscriptionId, version, … }]. */
  optIns: unknown;
  /** The step the submission was routed to (usually results), or an external URL. */
  routedStep: StepPath | null;
  routedUrl: string | null;
  /** The funnel's booking step, if it has one. */
  bookingStep: StepPath | null;
}

/** Body for POST /services/apexrest/barry/funnel-lead (BarryFunnelLead.cls). */
export interface FunnelLeadPayload {
  email: string;
  firstName: string | null;
  lastName: string | null;
  company: string | null;
  phone: string | null;
  title: string | null;
  leadSource: string;
  recordType: string | null;
  /** Owner for new Leads; without it the Integration User owns them. */
  ownerId: string | null;
  utmSource: string | null;
  utmMedium: string | null;
  utmCampaign: string | null;
  utmTerm: string | null;
  utmContent: string | null;
  campaignId: string | null;
  score: number | null;
  band: string | null;
  weakestAreas: string | null;
  answers: string;
  freeText: string | null;
  submissionUrl: string;
  /** Funnel slug: marks the Campaign as a funnel campaign and drives the results-email Flow. */
  funnelKey: string;
  funnelName: string;
  bandKey: string | null;
  /** The person's own results page (with their token). A new link = a new results email. */
  resultsUrl: string | null;
  /** Booking page for the review call: the results email's one call to action. */
  bookingUrl: string | null;
  weakestAreaDetails: WeakestAreaDetail[];
  /** Ticked opt-in boxes → OptIn CommSubscriptionConsents. */
  optIns: Array<{ subscriptionId: string; textVersion: string | null }>;
  /** When they submitted (ISO): the consent capture time. */
  capturedAt: string | null;
  /**
   * One-line summary for the Lead's Description (shown as "Subject" in the
   * new-lead Slack message), plus a link to the full answers.
   */
  summary: string;
}

export interface WeakestAreaDetail {
  label: string;
  diagnosis: string | null;
  cost: string | null;
}

const TEXT_TYPES = new Set(["shortText", "longText"]);

function str(v: unknown): string | null {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : null;
}

/** An answer as people read it: option labels, not stored values. */
export function answerLabel(q: QuestionDef, answer: unknown): string | null {
  const values = Array.isArray(answer) ? answer : [answer];
  const labels = values
    .filter((v): v is string => typeof v === "string" && v.trim() !== "")
    .map((v) => q.options?.find((o) => o.value === v)?.label ?? v.trim());
  return labels.length > 0 ? labels.join(", ") : null;
}

/** "Question: Answer" lines in quiz order, for the Funnel Answers long-text field. */
export function formatAnswers(questions: QuestionDef[], answers: Record<string, unknown>): string {
  return questions
    .map((q) => {
      const a = answerLabel(q, answers[q.key]);
      return a === null ? null : `${q.text}\n→ ${a}`;
    })
    .filter((line): line is string => line !== null)
    .join("\n\n");
}

/** Free-text answers only, for the Funnel Free Text field. */
export function formatFreeText(questions: QuestionDef[], answers: Record<string, unknown>): string | null {
  const parts = questions
    .filter((q) => TEXT_TYPES.has(q.type))
    .map((q) => {
      const a = str(answers[q.key]);
      return a ? `${q.text}\n${a}` : null;
    })
    .filter((p): p is string => p !== null);
  return parts.length > 0 ? parts.join("\n\n") : null;
}

interface SnapshotShape {
  score?: { band?: { key?: unknown; label?: unknown } | null; weakest?: Array<{ key?: unknown; label?: unknown }> };
  dimensions?: Array<{ key?: unknown; label?: unknown; diagnosis?: unknown; cost?: unknown }>;
}

export function bandKey(snapshot: unknown): string | null {
  return str((snapshot as SnapshotShape | null)?.score?.band?.key);
}

/** The weakest areas with the funnel's diagnosis and cost copy (as it was when they submitted). */
export function weakestAreaDetails(snapshot: unknown, count = 3): WeakestAreaDetail[] {
  const snap = snapshot as SnapshotShape | null;
  const weakest = snap?.score?.weakest;
  if (!Array.isArray(weakest)) return [];
  const dims = Array.isArray(snap?.dimensions) ? snap!.dimensions! : [];
  return weakest.slice(0, count).flatMap((w) => {
    const dim = dims.find((d) => d?.key === w?.key);
    const label = str(w?.label) ?? str(dim?.label);
    return label ? [{ label, diagnosis: str(dim?.diagnosis), cost: str(dim?.cost) }] : [];
  });
}

/** Public URL of a step for this submission, with its token. */
export function stepUrl(siteUrl: string, s: Pick<SubmissionForSync, "funnelSlug" | "funnelLive" | "token">, step: StepPath | null): string | null {
  if (!step) return null;
  const base = siteUrl.replace(/\/$/, "");
  const path = s.funnelLive && step.cleanPath ? `/${step.cleanPath}` : `/f/${s.funnelSlug}/${step.pathSegment}`;
  return `${base}${path}?s=${encodeURIComponent(s.token)}`;
}

/** FunnelSubmission.optIns → the Apex payload shape, ignoring anything malformed. */
export function optInsFor(raw: unknown): Array<{ subscriptionId: string; textVersion: string | null }> {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((o) => {
    const id = str((o as { subscriptionId?: unknown } | null)?.subscriptionId);
    return id && /^0Xl[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$/.test(id) ? [{ subscriptionId: id, textVersion: str((o as { version?: unknown }).version) }] : [];
  });
}

export function bandLabel(snapshot: unknown): string | null {
  return str((snapshot as SnapshotShape | null)?.score?.band?.label);
}

export function weakestAreas(snapshot: unknown, count = 3): string | null {
  const weakest = (snapshot as SnapshotShape | null)?.score?.weakest;
  if (!Array.isArray(weakest)) return null;
  const labels = weakest.slice(0, count).map((d) => str(d?.label)).filter((l): l is string => l !== null);
  return labels.length > 0 ? labels.join(", ") : null;
}

function submissionUrl(siteUrl: string, s: Pick<SubmissionForSync, "funnelSlug" | "id">): string {
  return `${siteUrl.replace(/\/$/, "")}/internal/funnels/${s.funnelSlug}/submissions/${s.id}`;
}

/** "crm" → "CRM", "companySize" → "Company size": a short label for an answer, from its question key. */
function keyLabel(key: string): string {
  if (key.length <= 4) return key.toUpperCase();
  const words = key.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").toLowerCase();
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/**
 * Who this is, at a glance, for the Lead's Description:
 *   Pipeline Leak Scorecard · 64/100 Leaking · Weakest: Pipeline, Forecasting, Configuration ·
 *   Source: linkedin (b1) · CRM: Salesforce · Founder/CEO · 20–50 people · Opted in: Black Cloud Signal
 *   Full answers: https://…/internal/funnels/…/submissions/…
 * Unscored single-choice answers (qualifiers such as CRM) are included by
 * their question key; scored answers are summarised by the score instead.
 */
export function leadSummary(s: SubmissionForSync, answersUrl: string): string {
  const parts: string[] = [s.funnelName];
  const band = bandLabel(s.resultSnapshot);
  if (s.scoreOverall !== null) parts.push(`${s.scoreOverall}/100${band ? ` ${band}` : ""}`);
  const weakest = weakestAreas(s.resultSnapshot);
  if (weakest) parts.push(`Weakest: ${weakest}`);
  const utm = s.utm ?? {};
  const source = str(utm.source);
  const content = str(utm.content);
  if (source) parts.push(`Source: ${source}${str(utm.medium) ? ` ${str(utm.medium)}` : ""}${content ? ` (${content})` : ""}`);
  else parts.push("Source: direct (no tracking link)");
  for (const q of s.questions) {
    if (q.dimensionKey) continue;
    if (q.type !== "single" && q.type !== "dropdown") continue;
    const a = answerLabel(q, s.answers[q.key]);
    if (a) parts.push(`${keyLabel(q.key)}: ${a}`);
  }
  if (s.role) parts.push(s.role);
  if (s.companySize) parts.push(`${s.companySize} people`);
  if (s.isFreeEmail) parts.push("Free email address");
  const optIns = Array.isArray(s.optIns)
    ? s.optIns.map((o) => str((o as { subscriptionName?: unknown } | null)?.subscriptionName)).filter((n): n is string => n !== null)
    : [];
  if (optIns.length > 0) parts.push(`Opted in: ${optIns.join(", ")}`);
  return `${parts.join(" · ")}\nFull answers: ${answersUrl}`;
}

/** utm_medium values that mean paid traffic (ads). Anything else, or no UTMs at all, is organic/direct. */
const PAID_MEDIUMS = new Set(["paid-social", "paid_social", "paidsocial", "paid", "cpc", "ppc", "cpm", "display", "paid-search", "paid_search", "ads", "ad"]);

export function isPaidTraffic(utm: Record<string, unknown> | null): boolean {
  const medium = str(utm?.medium)?.toLowerCase();
  return !!medium && PAID_MEDIUMS.has(medium);
}

/**
 * Lead Source: the funnel's own value (e.g. "Paid Social") for ad traffic;
 * the organic value (default "Web") for typed URLs, posts, email and other
 * untagged or non-paid visits, so organic leads aren't counted as ad leads.
 */
export function leadSourceFor(s: Pick<SubmissionForSync, "sfLeadSource" | "utm">, organic: string): string {
  return isPaidTraffic(s.utm) ? s.sfLeadSource : organic;
}

export function buildLeadPayload(
  s: SubmissionForSync,
  opts: { siteUrl: string; recordType: string | null; ownerId?: string | null; organicLeadSource?: string | null },
): FunnelLeadPayload {
  if (!s.email) throw new Error(`Submission ${s.id} has no email`);
  const utm = s.utm ?? {};
  return {
    email: s.email,
    firstName: s.firstName,
    lastName: s.lastName,
    company: s.company,
    phone: s.phone,
    title: s.role,
    leadSource: leadSourceFor(s, opts.organicLeadSource ?? "Web"),
    recordType: opts.recordType,
    ownerId: opts.ownerId ?? null,
    utmSource: str(utm.source),
    utmMedium: str(utm.medium),
    utmCampaign: str(utm.campaign),
    utmTerm: str(utm.term),
    utmContent: str(utm.content),
    campaignId: s.sfCampaignId,
    score: s.scoreOverall,
    band: bandLabel(s.resultSnapshot),
    weakestAreas: weakestAreas(s.resultSnapshot),
    answers: formatAnswers(s.questions, s.answers),
    freeText: formatFreeText(s.questions, s.answers),
    submissionUrl: submissionUrl(opts.siteUrl, s),
    funnelKey: s.funnelSlug,
    funnelName: s.funnelName,
    bandKey: bandKey(s.resultSnapshot),
    resultsUrl: s.routedUrl ?? stepUrl(opts.siteUrl, s, s.routedStep),
    bookingUrl: stepUrl(opts.siteUrl, s, s.bookingStep),
    weakestAreaDetails: weakestAreaDetails(s.resultSnapshot),
    optIns: optInsFor(s.optIns),
    capturedAt: s.completedAt ? new Date(s.completedAt).toISOString() : null,
    summary: leadSummary(s, submissionUrl(opts.siteUrl, s)),
  };
}

// ─── Google Calendar booking matching ────────────────────────────────────────

export interface CalendarEvent {
  id: string;
  status?: string;
  created?: string;
  attendees?: Array<{ email?: string; self?: boolean; organizer?: boolean; responseStatus?: string }>;
}

export interface CandidateSubmission {
  id: string;
  funnelId: string;
  email: string;
  completedAt: Date;
}

export interface BookingMatch {
  submissionId: string;
  funnelId: string;
  eventId: string;
  bookedAt: Date;
}

/**
 * Pairs new calendar bookings with funnel submissions by attendee email
 * (case-insensitive). A booking counts only if it was created after the
 * submission was completed (10 minutes' grace for clock skew). When someone
 * has several submissions, the most recent qualifying one gets the booking.
 * Each submission is matched at most once.
 */
export function matchBookings(events: CalendarEvent[], candidates: CandidateSubmission[], ownEmails: string[]): BookingMatch[] {
  const own = new Set(ownEmails.map((e) => e.toLowerCase()));
  const byEmail = new Map<string, CandidateSubmission[]>();
  for (const c of candidates) {
    const key = c.email.toLowerCase();
    byEmail.set(key, [...(byEmail.get(key) ?? []), c]);
  }
  const used = new Set<string>();
  const matches: BookingMatch[] = [];
  const GRACE_MS = 10 * 60 * 1000;

  for (const ev of events) {
    if (ev.status === "cancelled" || !ev.created) continue;
    const created = new Date(ev.created);
    if (Number.isNaN(created.getTime())) continue;
    for (const a of ev.attendees ?? []) {
      const email = a.email?.toLowerCase();
      if (!email || a.self || a.organizer || own.has(email)) continue;
      const pick = (byEmail.get(email) ?? [])
        .filter((c) => !used.has(c.id) && c.completedAt.getTime() <= created.getTime() + GRACE_MS)
        .sort((x, y) => y.completedAt.getTime() - x.completedAt.getTime())[0];
      if (!pick) continue;
      used.add(pick.id);
      matches.push({ submissionId: pick.id, funnelId: pick.funnelId, eventId: ev.id, bookedAt: created });
    }
  }
  return matches;
}
