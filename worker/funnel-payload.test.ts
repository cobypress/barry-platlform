import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  buildLeadPayload,
  formatAnswers,
  formatFreeText,
  matchBookings,
  optInsFor,
  stepUrl,
  weakestAreaDetails,
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
    weakest: [{ key: "pipeline", label: "Pipeline" }, { key: "data", label: "Data quality" }, { key: "config", label: "Configuration" }, { key: "forecast", label: "Forecasting" }],
  },
  dimensions: [
    { key: "pipeline", label: "Pipeline", diagnosis: "Deals sit without next steps.", cost: "Stalled deals go unnoticed." },
    { key: "data", label: "Data quality", diagnosis: "Duplicates and empty key fields.", cost: null },
    { key: "config", label: "Configuration", diagnosis: "", cost: "" },
  ],
};

function submission(over: Partial<SubmissionForSync> = {}): SubmissionForSync {
  return {
    id: "sub1",
    token: "tok_abc-123",
    funnelLive: true,
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
    completedAt: new Date("2026-10-07T09:40:00Z"),
    optIns: [{ subscriptionId: "0XlQH00000029AT0AY", subscriptionName: "Black Cloud Signal", label: "Send me…", version: "signal-v1" }],
    routedStep: { pathSegment: "results", cleanPath: null },
    routedUrl: null,
    bookingStep: { pathSegment: "book", cleanPath: null },
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

describe("consent + results email fields", () => {
  test("funnel key, band key, links with the token, weakest-area copy, opt-ins and capture time", () => {
    const p = buildLeadPayload(submission(), { siteUrl: "https://black-cloud.com/", recordType: null });
    assert.equal(p.funnelKey, "pipeline-leak-scorecard");
    assert.equal(p.funnelName, "Pipeline Leak Scorecard (LinkedIn)");
    assert.equal(p.bandKey, "leaking");
    assert.equal(p.resultsUrl, "https://black-cloud.com/f/pipeline-leak-scorecard/results?s=tok_abc-123");
    assert.equal(p.bookingUrl, "https://black-cloud.com/f/pipeline-leak-scorecard/book?s=tok_abc-123");
    assert.deepEqual(p.weakestAreaDetails, [
      { label: "Pipeline", diagnosis: "Deals sit without next steps.", cost: "Stalled deals go unnoticed." },
      { label: "Data quality", diagnosis: "Duplicates and empty key fields.", cost: null },
      { label: "Configuration", diagnosis: null, cost: null },
    ]);
    assert.deepEqual(p.optIns, [{ subscriptionId: "0XlQH00000029AT0AY", textVersion: "signal-v1" }]);
    assert.equal(p.capturedAt, "2026-10-07T09:40:00.000Z");
  });

  test("external routing keeps the URL; no booking step means no booking link", () => {
    const p = buildLeadPayload(submission({ routedUrl: "https://example.com/thanks", bookingStep: null }), { siteUrl: "https://black-cloud.com", recordType: null });
    assert.equal(p.resultsUrl, "https://example.com/thanks");
    assert.equal(p.bookingUrl, null);
  });

  test("clean paths only when the funnel is live", () => {
    const step = { pathSegment: "start", cleanPath: "scorecard" };
    assert.equal(stepUrl("https://black-cloud.com", { funnelSlug: "f", funnelLive: true, token: "t" }, step), "https://black-cloud.com/scorecard?s=t");
    assert.equal(stepUrl("https://black-cloud.com", { funnelSlug: "f", funnelLive: false, token: "t" }, step), "https://black-cloud.com/f/f/start?s=t");
  });

  test("malformed opt-ins and snapshots are ignored", () => {
    assert.deepEqual(optInsFor([{ subscriptionId: "701QH00000AbCdE" }, null, "x", { subscriptionId: "0XlQH00000029AT" }]), [{ subscriptionId: "0XlQH00000029AT", textVersion: null }]);
    assert.deepEqual(optInsFor(null), []);
    assert.deepEqual(weakestAreaDetails(null), []);
    assert.deepEqual(weakestAreaDetails({ score: { weakest: [{ key: "x", label: "X" }] } }), [{ label: "X", diagnosis: null, cost: null }]);
  });
});

