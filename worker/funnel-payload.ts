// Builds the Salesforce payload for a completed funnel submission.
// Pure (no imports), so it's unit-tested with `node --test`.

export interface QuestionDef {
  key: string;
  text: string;
  type: string;
  options?: Array<{ label: string; value: string }>;
}

export interface SubmissionForSync {
  id: string;
  funnelName: string;
  funnelSlug: string;
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
  utm: Record<string, unknown> | null;
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
  score?: { band?: { label?: unknown } | null; weakest?: Array<{ label?: unknown }> };
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

export function buildLeadPayload(
  s: SubmissionForSync,
  opts: { siteUrl: string; recordType: string | null; ownerId?: string | null },
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
    leadSource: s.sfLeadSource,
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
    submissionUrl: `${opts.siteUrl.replace(/\/$/, "")}/internal/funnels/${s.funnelSlug}/submissions/${s.id}`,
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

// ─── Kit ─────────────────────────────────────────────────────────────────────

/**
 * Tags applied in Kit for a completed submission: "<prefix>-completed" and
 * "<prefix>-<band key>" (e.g. scorecard-completed, scorecard-leaking). Kit
 * sequences hang off these. Empty when the funnel has no tag prefix.
 */
export function kitTags(prefix: string | null, bandKey: string | null): string[] {
  const p = (prefix ?? "").trim().toLowerCase();
  if (!p) return [];
  return [`${p}-completed`, ...(bandKey ? [`${p}-${bandKey.toLowerCase()}`] : [])];
}
