import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  buildLeadPayload,
  formatAnswers,
  formatFreeText,
  kitTags,
  matchBookings,
  weakestAreas,
  type CandidateSubmission,
  type QuestionDef,
  type SubmissionForSync,
} from "./funnel-payload.ts";

const questions: QuestionDef[] = [
  { key: "q1", text: "Every open opportunity has a next step and a date", type: "scale", options: [{ label: "Always", value: "always" }, { label: "Mostly", value: "mostly" }] },
  { key: "crm", text: "Which CRM do you use?", type: "single", options: [{ label: "HubSpot", value: "hubspot" }] },
  { key: "tools", text: "Tools", type: "multi", options: [{ label: "Slack", value: "slack" }, { label: "Gong", value: "gong" }] },
  { key: "changeTomorrow", text: "What would you change tomorrow?", type: "longText" },
  { key: "skipped", text: "Optional", type: "single", options: [{ label: "Yes", value: "yes" }] },
];

const snapshot = {
  score: {
    overall: 64,
    band: { key: "leaking", label: "Leaking" },
    weakest: [{ label: "Pipeline" }, { label: "Data quality" }, { label: "Configuration" }, { label: "Forecasting" }],
  },
};

function submission(over: Partial<SubmissionForSync> = {}): SubmissionForSync {
  return {
    id: "sub1",
    funnelName: "Pipeline Leak Scorecard (LinkedIn)",
    funnelSlug: "pipeline-leak-scorecard",
    sfCampaignId: "701QH00000AbCdE",
    sfLeadSource: "Paid Social",
    questions,
    answers: { q1: "mostly", crm: "hubspot", tools: ["slack", "gong"], changeTomorrow: "  Forecasting  " },
    scoreOverall: 64,
    resultSnapshot: snapshot,
    firstName: "Sam",
    lastName: null,
    email: "sam@acme.co.uk",
    company: "Acme",
    role: "Sales Director/Head of Sales",
    phone: null,
    utm: { source: "linkedin", medium: "paid-social", campaign: "", term: null },
    ...over,
  };
}

describe("payload", () => {
  test("answers use question text and option labels, in order, skipping blanks", () => {
    assert.equal(
      formatAnswers(questions, submission().answers),
      "Every open opportunity has a next step and a date\n→ Mostly\n\nWhich CRM do you use?\n→ HubSpot\n\nTools\n→ Slack, Gong\n\nWhat would you change tomorrow?\n→ Forecasting",
    );
  });

  test("free text only includes text questions", () => {
    assert.equal(formatFreeText(questions, submission().answers), "What would you change tomorrow?\nForecasting");
    assert.equal(formatFreeText(questions, { q1: "always" }), null);
  });

  test("weakest areas: top three, comma-separated", () => {
    assert.equal(weakestAreas(snapshot), "Pipeline, Data quality, Configuration");
    assert.equal(weakestAreas(null), null);
  });

  test("buildLeadPayload maps everything", () => {
    const p = buildLeadPayload(submission(), { siteUrl: "https://black-cloud.com/", recordType: "B2B_Prospect", ownerId: "005QH000004BflVYAS" });
    assert.equal(p.ownerId, "005QH000004BflVYAS");
    assert.equal(p.email, "sam@acme.co.uk");
    assert.equal(p.title, "Sales Director/Head of Sales");
    assert.equal(p.leadSource, "Paid Social");
    assert.equal(p.utmSource, "linkedin");
    assert.equal(p.utmCampaign, null, "blank UTMs become null");
    assert.equal(p.band, "Leaking");
    assert.equal(p.score, 64);
    assert.equal(p.campaignId, "701QH00000AbCdE");
    assert.equal(p.submissionUrl, "https://black-cloud.com/internal/funnels/pipeline-leak-scorecard/submissions/sub1");
    assert.equal(JSON.stringify(p).includes("undefined"), false);
  });

  test("refuses submissions without an email", () => {
    assert.throws(() => buildLeadPayload(submission({ email: null }), { siteUrl: "x", recordType: null }), /no email/);
  });
});

describe("matchBookings", () => {
  const at = (iso: string) => new Date(iso);
  const candidates: CandidateSubmission[] = [
    { id: "old", funnelId: "f", email: "Sam@Acme.co.uk", completedAt: at("2026-10-01T10:00:00Z") },
    { id: "new", funnelId: "f", email: "sam@acme.co.uk", completedAt: at("2026-10-06T10:00:00Z") },
    { id: "later", funnelId: "f", email: "late@acme.co.uk", completedAt: at("2026-10-06T12:00:00Z") },
  ];

  test("matches attendee email case-insensitively to the latest submission before the booking", () => {
    const m = matchBookings(
      [{ id: "e1", created: "2026-10-06T10:30:00Z", attendees: [{ email: "coby@black-cloud.com", organizer: true }, { email: "SAM@acme.co.uk" }] }],
      candidates,
      ["coby@black-cloud.com"],
    );
    assert.deepEqual(m.map((x) => [x.submissionId, x.eventId]), [["new", "e1"]]);
    assert.equal(m[0].bookedAt.toISOString(), "2026-10-06T10:30:00.000Z");
  });

  test("ignores bookings made before the submission (beyond 10 minutes' grace)", () => {
    const m = matchBookings([{ id: "e2", created: "2026-10-06T11:00:00Z", attendees: [{ email: "late@acme.co.uk" }] }], candidates, []);
    assert.deepEqual(m, []);
    const grace = matchBookings([{ id: "e3", created: "2026-10-06T11:55:00Z", attendees: [{ email: "late@acme.co.uk" }] }], candidates, []);
    assert.deepEqual(grace.map((x) => x.submissionId), ["later"]);
  });

  test("ignores cancelled events, own addresses and self; matches each submission once", () => {
    const m = matchBookings(
      [
        { id: "x", status: "cancelled", created: "2026-10-06T10:30:00Z", attendees: [{ email: "sam@acme.co.uk" }] },
        { id: "y", created: "2026-10-06T10:31:00Z", attendees: [{ email: "sam@acme.co.uk", self: true }, { email: "coby@black-cloud.com" }] },
        { id: "a", created: "2026-10-06T10:32:00Z", attendees: [{ email: "sam@acme.co.uk" }] },
        { id: "b", created: "2026-10-06T10:33:00Z", attendees: [{ email: "sam@acme.co.uk" }] },
      ],
      candidates,
      ["coby@black-cloud.com"],
    );
    // "a" takes the newest submission; "b" then takes the older one.
    assert.deepEqual(m.map((x) => [x.eventId, x.submissionId]), [["a", "new"], ["b", "old"]]);
  });
});

describe("kitTags", () => {
  test("prefix + completed + band", () => {
    assert.deepEqual(kitTags("scorecard", "leaking"), ["scorecard-completed", "scorecard-leaking"]);
    assert.deepEqual(kitTags(" Scorecard ", "Critical"), ["scorecard-completed", "scorecard-critical"]);
  });
  test("no prefix → no tags; no band → completed only", () => {
    assert.deepEqual(kitTags("", "leaking"), []);
    assert.deepEqual(kitTags(null, "leaking"), []);
    assert.deepEqual(kitTags("scorecard", null), ["scorecard-completed"]);
  });
});
