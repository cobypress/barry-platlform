// Funnel Builder jobs (black-cloud.com/f/… → Salesforce, Google Calendar).
//
//   funnel-submission-sync  { submissionId }  Lead/Contact + Campaign Member via /barry/funnel-lead
//   funnel-booking-sync     { submissionId }  Campaign Member → "Meeting Booked" via /barry/funnel-booking
//   funnel-booking-poll     (every 5 min)     match new Google appointment bookings to submissions
//   funnel-kit-sync         { submissionId }  Kit subscriber + tags (<prefix>-completed, <prefix>-<band>)
//
// Only submission ids travel through Redis; everything else is read from
// Neon here, so no personal data sits in the queue.

import { createSign } from "crypto";
import type { Pool } from "pg";
import type { Queue } from "bullmq";
import { salesforce } from "./salesforce";
import {
  buildLeadPayload,
  kitTags,
  matchBookings,
  type CalendarEvent,
  type CandidateSubmission,
  type QuestionDef,
  type SubmissionForSync,
} from "./funnel-payload";

export const FUNNEL_JOBS = {
  submissionSync: "funnel-submission-sync",
  bookingSync: "funnel-booking-sync",
  bookingPoll: "funnel-booking-poll",
  kitSync: "funnel-kit-sync",
} as const;

export const FUNNEL_JOB_OPTS = {
  attempts: 5,
  backoff: { type: "exponential" as const, delay: 30_000 },
  removeOnComplete: true,
  removeOnFail: 200,
};

const SITE_URL = process.env.SITE_URL || "https://black-cloud.com";

interface Deps {
  pool: Pool;
  queue: Queue;
}

function message(err: unknown): string {
  return (err instanceof Error ? err.message : String(err)).slice(0, 1000);
}

function submissionIdOf(data: unknown): string {
  const id = (data as { submissionId?: unknown } | null)?.submissionId;
  if (typeof id !== "string" || !id) throw new Error("submissionId is required");
  return id;
}

export async function handleFunnelJob(name: string, data: unknown, deps: Deps): Promise<unknown> {
  switch (name) {
    case FUNNEL_JOBS.submissionSync:
      return syncSubmission(submissionIdOf(data), deps);
    case FUNNEL_JOBS.bookingSync:
      return syncBooking(submissionIdOf(data), deps);
    case FUNNEL_JOBS.bookingPoll:
      return pollBookings(deps);
    case FUNNEL_JOBS.kitSync:
      return syncKit(submissionIdOf(data), deps);
    default:
      throw new Error(`Unknown funnel job ${name}`);
  }
}

// ─── Salesforce: lead + campaign member ──────────────────────────────────────

interface FunnelLeadResponse {
  success: boolean;
  matchedBy?: string;
  leadId?: string;
  contactId?: string;
  campaignMemberId?: string;
  error?: string;
}

async function syncSubmission(submissionId: string, { pool }: Deps) {
  const { rows } = await pool.query(
    `SELECT s.id, s.status, s."isTest", s.answers, s."scoreOverall", s."resultSnapshot",
            s."firstName", s."lastName", s.email, s.company, s.role, s.phone, s.utm,
            f.name AS "funnelName", f.slug AS "funnelSlug", f."sfCampaignId", f."sfLeadSource",
            q.questions
       FROM "FunnelSubmission" s
       JOIN "Funnel" f ON f.id = s."funnelId"
       LEFT JOIN "FunnelQuiz" q ON q.id = s."quizId"
      WHERE s.id = $1`,
    [submissionId],
  );
  const row = rows[0];
  if (!row) return { skipped: "submission not found" };
  if (row.isTest || row.status !== "completed") {
    await pool.query(`UPDATE "FunnelSubmission" SET "sfStatus" = 'skipped', "updatedAt" = now() WHERE id = $1`, [submissionId]);
    return { skipped: row.isTest ? "test submission" : "not completed" };
  }

  const submission: SubmissionForSync = {
    id: row.id,
    funnelName: row.funnelName,
    funnelSlug: row.funnelSlug,
    sfCampaignId: row.sfCampaignId,
    sfLeadSource: row.sfLeadSource,
    questions: (Array.isArray(row.questions) ? row.questions : []) as QuestionDef[],
    answers: (row.answers ?? {}) as Record<string, unknown>,
    scoreOverall: row.scoreOverall,
    resultSnapshot: row.resultSnapshot,
    firstName: row.firstName,
    lastName: row.lastName,
    email: row.email,
    company: row.company,
    role: row.role,
    phone: row.phone,
    utm: row.utm,
  };

  try {
    const payload = buildLeadPayload(submission, {
      siteUrl: SITE_URL,
      recordType: process.env.FUNNEL_LEAD_RECORD_TYPE || null,
      ownerId: process.env.FUNNEL_LEAD_OWNER_ID || null,
    });
    const res = await salesforce.sfJson<FunnelLeadResponse>("/services/apexrest/barry/funnel-lead", {
      method: "POST",
      body: JSON.stringify(payload),
      headers: { "Content-Type": "application/json; charset=utf-8" },
    });
    if (!res.success) throw new Error(res.error || "Salesforce rejected the lead");

    await pool.query(
      `UPDATE "FunnelSubmission"
          SET "sfStatus" = 'synced', "sfLeadId" = $2, "sfContactId" = $3, "sfCampaignMemberId" = $4,
              "sfError" = NULL, "sfSyncedAt" = now(), "sfAttempts" = "sfAttempts" + 1, "updatedAt" = now()
        WHERE id = $1`,
      [submissionId, res.leadId ?? null, res.contactId ?? null, res.campaignMemberId ?? null],
    );
    return { matchedBy: res.matchedBy, leadId: res.leadId, contactId: res.contactId };
  } catch (err) {
    await pool.query(
      `UPDATE "FunnelSubmission" SET "sfStatus" = 'failed', "sfError" = $2, "sfAttempts" = "sfAttempts" + 1, "updatedAt" = now() WHERE id = $1`,
      [submissionId, message(err)],
    );
    throw err; // BullMQ retries with backoff
  }
}

// ─── Salesforce: booking ─────────────────────────────────────────────────────

async function syncBooking(submissionId: string, { pool }: Deps) {
  const { rows } = await pool.query(
    `SELECT s."sfStatus", s."sfLeadId", s."sfContactId", s."isTest", f."sfCampaignId"
       FROM "FunnelSubmission" s JOIN "Funnel" f ON f.id = s."funnelId" WHERE s.id = $1`,
    [submissionId],
  );
  const row = rows[0];
  if (!row || row.isTest) return { skipped: "not found or test" };
  if (!row.sfCampaignId) return { skipped: "funnel has no Salesforce Campaign" };
  if (!row.sfLeadId && !row.sfContactId) {
    // The lead sync hasn't finished (or failed): retry later via backoff.
    throw new Error(`Lead not synced yet (sfStatus=${row.sfStatus}); will retry`);
  }
  const res = await salesforce.sfJson<{ success: boolean; error?: string }>("/services/apexrest/barry/funnel-booking", {
    method: "POST",
    body: JSON.stringify({ campaignId: row.sfCampaignId, leadId: row.sfLeadId, contactId: row.sfContactId }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
  if (!res.success) throw new Error(res.error || "Salesforce rejected the booking update");
  return { booked: true };
}

// ─── Kit (email sequences) ───────────────────────────────────────────────────

const KIT_API = "https://api.kit.com/v4";

async function kitRequest<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${KIT_API}${path}`, {
    method: "POST",
    headers: { "X-Kit-Api-Key": process.env.KIT_API_KEY ?? "", "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Kit ${path} failed (${res.status}): ${text.slice(0, 300)}`);
  return (text ? JSON.parse(text) : {}) as T;
}

/**
 * Adds or updates the person in Kit and applies the funnel's tags; Kit's own
 * automations start the sequences. Only for people who ticked the consent box.
 * Kit's create-subscriber call is an upsert that never reactivates someone who
 * unsubscribed, so unsubscribes are respected.
 */
async function syncKit(submissionId: string, { pool }: Deps) {
  const { rows } = await pool.query(
    `SELECT s.status, s."isTest", s."consentGiven", s.email, s."firstName", s."bandKey", f."kitTagPrefix"
       FROM "FunnelSubmission" s JOIN "Funnel" f ON f.id = s."funnelId" WHERE s.id = $1`,
    [submissionId],
  );
  const row = rows[0];
  const skip = async (reason: string) => {
    await pool.query(`UPDATE "FunnelSubmission" SET "kitStatus" = 'skipped', "kitError" = $2, "updatedAt" = now() WHERE id = $1`, [submissionId, reason]);
    return { skipped: reason };
  };
  if (!row) return { skipped: "submission not found" };
  if (row.isTest) return skip("Test submission");
  if (row.status !== "completed" || !row.email) return skip("Not completed");
  if (!row.consentGiven) return skip("No consent");
  const tags = kitTags(row.kitTagPrefix, row.bandKey);
  if (tags.length === 0) return skip("Funnel has no Kit tag prefix");
  if (!process.env.KIT_API_KEY) return skip("KIT_API_KEY isn't set on the worker");

  try {
    const sub = await kitRequest<{ subscriber?: { id?: number } }>("/subscribers", {
      email_address: row.email,
      first_name: row.firstName ?? undefined,
      state: "active",
    });
    for (const name of tags) {
      const tag = await kitRequest<{ tag?: { id?: number } }>("/tags", { name });
      if (!tag.tag?.id) throw new Error(`Kit didn't return an id for tag "${name}"`);
      await kitRequest(`/tags/${tag.tag.id}/subscribers`, { email_address: row.email });
    }
    await pool.query(
      `UPDATE "FunnelSubmission"
          SET "kitStatus" = 'synced', "kitSubscriberId" = $2, "kitError" = NULL, "kitSyncedAt" = now(),
              "kitAttempts" = "kitAttempts" + 1, "updatedAt" = now()
        WHERE id = $1`,
      [submissionId, sub.subscriber?.id != null ? String(sub.subscriber.id) : null],
    );
    return { tags };
  } catch (err) {
    await pool.query(
      `UPDATE "FunnelSubmission" SET "kitStatus" = 'failed', "kitError" = $2, "kitAttempts" = "kitAttempts" + 1, "updatedAt" = now() WHERE id = $1`,
      [submissionId, message(err)],
    );
    throw err;
  }
}

// ─── Google Calendar booking poll ────────────────────────────────────────────

interface GoogleConfig {
  clientEmail: string;
  privateKey: string;
  calendarId: string;
  /** Domain-wide delegation fallback: the user to impersonate. */
  subject: string | null;
}

export function googleConfig(): GoogleConfig | null {
  const clientEmail = process.env.GOOGLE_SA_EMAIL;
  const rawKey = process.env.GOOGLE_SA_PRIVATE_KEY;
  const calendarId = process.env.GOOGLE_CALENDAR_ID;
  if (!clientEmail || !rawKey || !calendarId) return null;
  return {
    clientEmail,
    // Env vars usually hold the key with literal "\n"s.
    privateKey: rawKey.replace(/\\n/g, "\n"),
    calendarId,
    subject: process.env.GOOGLE_IMPERSONATE || null,
  };
}

let tokenCache: { token: string; expiresAt: number } | null = null;

function base64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64").replace(/=+$/, "").replace(/\+/g, "-").replace(/\//g, "_");
}

/** Service-account OAuth (JWT bearer), read-only Calendar scope. No googleapis dependency. */
async function googleAccessToken(cfg: GoogleConfig): Promise<string> {
  if (tokenCache && tokenCache.expiresAt > Date.now() + 60_000) return tokenCache.token;
  const now = Math.floor(Date.now() / 1000);
  const claims: Record<string, unknown> = {
    iss: cfg.clientEmail,
    scope: "https://www.googleapis.com/auth/calendar.readonly",
    aud: "https://oauth2.googleapis.com/token",
    iat: now,
    exp: now + 3600,
  };
  if (cfg.subject) claims.sub = cfg.subject;
  const unsigned = `${base64url(JSON.stringify({ alg: "RS256", typ: "JWT" }))}.${base64url(JSON.stringify(claims))}`;
  const signature = createSign("RSA-SHA256").update(unsigned).sign(cfg.privateKey);
  const assertion = `${unsigned}.${base64url(signature)}`;

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion }),
  });
  const body = (await res.json().catch(() => ({}))) as { access_token?: string; expires_in?: number; error?: string; error_description?: string };
  if (!res.ok || !body.access_token) {
    throw new Error(`Google token request failed (${res.status}): ${body.error_description || body.error || "unknown"}`);
  }
  tokenCache = { token: body.access_token, expiresAt: Date.now() + (body.expires_in ?? 3600) * 1000 };
  return body.access_token;
}

async function listUpdatedEvents(cfg: GoogleConfig, updatedMin: Date): Promise<CalendarEvent[]> {
  const token = await googleAccessToken(cfg);
  const events: CalendarEvent[] = [];
  let pageToken: string | undefined;
  do {
    const params = new URLSearchParams({
      updatedMin: updatedMin.toISOString(),
      singleEvents: "true",
      showDeleted: "false",
      maxResults: "250",
    });
    if (pageToken) params.set("pageToken", pageToken);
    const res = await fetch(
      `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(cfg.calendarId)}/events?${params}`,
      { headers: { Authorization: `Bearer ${token}` } },
    );
    const body = (await res.json().catch(() => ({}))) as { items?: CalendarEvent[]; nextPageToken?: string; error?: { message?: string } };
    if (!res.ok) throw new Error(`Google Calendar events.list failed (${res.status}): ${body.error?.message ?? "unknown"}`);
    events.push(...(body.items ?? []));
    pageToken = body.nextPageToken;
  } while (pageToken);
  return events;
}

/**
 * Finds Google appointment bookings made since the last poll and marks the
 * matching funnel submissions as booked. The cursor (last poll start) lives
 * in FunnelSyncCursor; polls overlap by 2 minutes and matching skips anything
 * already booked, so nothing is missed or double-counted.
 */
async function pollBookings({ pool, queue }: Deps) {
  const cfg = googleConfig();
  if (!cfg) return { skipped: "Google Calendar not configured" };

  const cursorKey = `google-calendar:${cfg.calendarId}`;
  const startedAt = new Date();
  const { rows: cursorRows } = await pool.query(`SELECT value FROM "FunnelSyncCursor" WHERE key = $1`, [cursorKey]);
  const since = cursorRows[0] ? new Date(cursorRows[0].value) : new Date(startedAt.getTime() - 24 * 3600 * 1000);

  const events = await listUpdatedEvents(cfg, new Date(since.getTime() - 2 * 60 * 1000));
  const emails = [
    ...new Set(
      events.flatMap((e) => (e.attendees ?? []).map((a) => a.email?.toLowerCase()).filter((x): x is string => !!x)),
    ),
  ];

  let matched = 0;
  if (emails.length > 0) {
    const { rows } = await pool.query(
      `SELECT id, "funnelId", email, "completedAt" FROM "FunnelSubmission"
        WHERE status = 'completed' AND "isTest" = false AND "bookedAt" IS NULL
          AND lower(email) = ANY($1) AND "completedAt" > now() - interval '30 days'`,
      [emails],
    );
    const candidates: CandidateSubmission[] = rows.map((r) => ({
      id: r.id,
      funnelId: r.funnelId,
      email: r.email,
      completedAt: new Date(r.completedAt),
    }));
    const ownEmails = [cfg.calendarId, cfg.clientEmail, ...(cfg.subject ? [cfg.subject] : [])];

    for (const m of matchBookings(events, candidates, ownEmails)) {
      const updated = await pool.query(
        `UPDATE "FunnelSubmission" SET "bookedAt" = $2, "bookingSource" = 'google', "updatedAt" = now()
          WHERE id = $1 AND "bookedAt" IS NULL`,
        [m.submissionId, m.bookedAt],
      );
      if (updated.rowCount !== 1) continue;
      matched++;
      await pool.query(
        `INSERT INTO "FunnelEvent" ("funnelId", "submissionId", type, destination) VALUES ($1, $2, 'booking_complete', $3)`,
        [m.funnelId, m.submissionId, `google:${m.eventId}`],
      );
      await queue.add(FUNNEL_JOBS.bookingSync, { submissionId: m.submissionId }, { ...FUNNEL_JOB_OPTS, jobId: `funnel-booking-sync-${m.submissionId}` });
    }
  }

  await pool.query(
    `INSERT INTO "FunnelSyncCursor" (key, value, "updatedAt") VALUES ($1, $2, now())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "updatedAt" = now()`,
    [cursorKey, startedAt.toISOString()],
  );
  return { events: events.length, matched };
}

/** Registers (idempotently) the 5-minute booking poll when Google Calendar is configured. */
export async function scheduleBookingPoll(queue: Queue): Promise<void> {
  if (!googleConfig()) {
    console.log("[funnels] Google Calendar not configured (GOOGLE_SA_EMAIL/GOOGLE_SA_PRIVATE_KEY/GOOGLE_CALENDAR_ID): booking poll off");
    return;
  }
  await queue.upsertJobScheduler(
    FUNNEL_JOBS.bookingPoll,
    { every: 5 * 60 * 1000 },
    { name: FUNNEL_JOBS.bookingPoll, data: {}, opts: { removeOnComplete: true, removeOnFail: 50 } },
  );
  console.log("[funnels] Booking poll scheduled every 5 minutes");
}
