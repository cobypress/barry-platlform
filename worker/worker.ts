import "dotenv/config";
import { Worker, Job } from "bullmq";
import { URL } from "node:url";
import { pool } from "./db";
import { salesforce } from "./salesforce";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name} in environment variables`);
  return v;
}

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

const redisUrl = requireEnv("REDIS_URL");

// ---- Parse Redis URL safely ----
const parsed = new URL(redisUrl);

const connection: any = {
  host: parsed.hostname,
  port: parsed.port ? Number(parsed.port) : 6379,
  password: parsed.password || undefined,
};

if (parsed.protocol === "rediss:") {
  connection.tls = {};
}

console.log("Redis config:", {
  protocol: parsed.protocol,
  host: parsed.hostname,
  port: parsed.port,
  hasPassword: Boolean(parsed.password),
});

console.log("🚀 Worker starting... listening on queue: barry-jobs");

// ---- Slack helpers ----
type SlackResponseUrlBody = {
  response_type?: "ephemeral" | "in_channel";
  text?: string;
  blocks?: unknown[];
  replace_original?: boolean;
};

async function replyToResponseUrl(responseUrl: string, body: SlackResponseUrlBody) {
  const res = await fetch(responseUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Failed to respond via response_url: ${res.status} ${t}`);
  }
}

async function slackUpdateMessage(token: string, channel: string, ts: string, text: string) {
  const res = await fetch("https://slack.com/api/chat.update", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json; charset=utf-8",
    },
    body: JSON.stringify({
      channel,
      ts,
      text,
      blocks: [{ type: "section", text: { type: "mrkdwn", text } }],
    }),
  });

  const data = (await res.json()) as { ok: boolean; error?: string };
  if (!data.ok) throw new Error(`Slack chat.update failed: ${data.error}`);
}

// ---- Salesforce helpers ----

type ValidateUserResponse = {
  status: "channel_not_linked" | "no_entitlement" | "contact_not_found" | "pending_approval" | "approved";
  accountId?: string;
  contactId?: string;
};

async function sfValidateUser(
  slackTeamId: string,
  slackChannelId: string,
  email: string,
  slackUserId: string
): Promise<ValidateUserResponse> {
  return salesforce.sfJson<ValidateUserResponse>("/services/apexrest/barry/validate-user", {
    method: "POST",
    body: JSON.stringify({ slackTeamId, slackChannelId, email, slackUserId }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

type CreateContactRequest = {
  accountId: string;
  email: string;
  firstName: string;
  lastName: string;
  phone?: string;
  jobTitle?: string;
  slackUserId: string;
  slackTeamId: string;
};

type CreateContactResponse = {
  success: boolean;
  contactId?: string;
  error?: string;
};

async function sfCreateContact(data: CreateContactRequest): Promise<CreateContactResponse> {
  return salesforce.sfJson<CreateContactResponse>("/services/apexrest/barry/create-contact", {
    method: "POST",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

// ── Case list constants ───────────────────────────────────────────────────────
const CASES_PAGE_SIZE = 5;

const STATUS_ORDER: Record<string, number> = {
  "New": 0, "In Progress": 1, "Waiting on Client": 2, "Client Responded": 3,
  "On Hold": 4, "Escalated": 5, "Waiting to be Closed": 6, "Re-opened": 7, "Closed": 8,
};

const STATUS_EMOJI: Record<string, string> = {
  "New": "👋", "In Progress": "👀", "Waiting on Client": "🫵", "Client Responded": "😨",
  "On Hold": "🚫", "Escalated": "⬆️", "Waiting to be Closed": "🫡", "Re-opened": "⚠️", "Closed": "🤝",
};

type SFCase = {
  id: string; caseNumber: string; subject: string;
  status: string; priority: string; type: string; createdDate: string;
  contactName?: string;
};

type GetCasesResponse = { success: boolean; cases?: SFCase[]; error?: string };

async function sfGetCases(accountId: string): Promise<GetCasesResponse> {
  return salesforce.sfJson<GetCasesResponse>("/services/apexrest/barry/cases", {
    method: "POST",
    body: JSON.stringify({ accountId }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

// ─────────────────────────────────────────────────────────────────────────────

type CreateCaseRequest = {
  accountId: string;
  contactId: string;
  subject: string;
  description: string;
  priority: string;
  type: string;
};

type CreateCaseResponse = {
  success: boolean;
  caseId?: string;
  caseNumber?: string;
  error?: string;
};

async function sfCreateCase(data: CreateCaseRequest): Promise<CreateCaseResponse> {
  return salesforce.sfJson<CreateCaseResponse>("/services/apexrest/barry/create-case", {
    method: "POST",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

type CloseCaseResponse = { success: boolean; error?: string };

async function sfCloseCase(caseId: string): Promise<CloseCaseResponse> {
  return salesforce.sfJson<CloseCaseResponse>("/services/apexrest/barry/close-case", {
    method: "POST",
    body: JSON.stringify({ caseId }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

async function sfSaveCsat(caseId: string, score: number): Promise<{ success: boolean; error?: string }> {
  return salesforce.sfJson("/services/apexrest/barry/save-csat", {
    method: "POST",
    body: JSON.stringify({ caseId, score }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

async function sfSaveCsatFeedback(caseId: string, feedback: string): Promise<{ success: boolean; error?: string }> {
  return salesforce.sfJson("/services/apexrest/barry/save-csat-feedback", {
    method: "POST",
    body: JSON.stringify({ caseId, feedback }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

// ---- Shared validation result handler ----
type UserContext = {
  team_id: string;
  user_id: string;
  email: string;
  channel_id: string;
  response_url: string;
};

async function handleValidationResult(result: ValidateUserResponse, ctx: UserContext): Promise<void> {
  const { team_id, user_id, email, channel_id, response_url } = ctx;

  switch (result.status) {
    case "channel_not_linked": {
      // Private: replace the ephemeral with a brief note
      await replyToResponseUrl(response_url, {
        replace_original: true,
        text: "❌ This channel isn't connected to a customer account yet.",
      });
      // Public: post to the channel so the Account Manager can action it
      const token = requireEnv("SLACK_BOT_TOKEN");
      await fetch("https://slack.com/api/chat.postMessage", {
        method: "POST",
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          channel: channel_id,
          text: "⚠️ This Slack channel hasn't been linked to a Salesforce account yet. An Account Manager will need to set it up before cases can be raised here.",
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: "⚠️ *This channel isn't connected to a Salesforce account.*\n\nBefore support cases can be raised here, an Account Manager needs to link this channel to the relevant customer account in Salesforce.\n\nPlease reach out to your Account Manager to get this set up.",
              },
            },
          ],
        }),
      });
      break;
    }

    case "no_entitlement":
      await replyToResponseUrl(response_url, {
        replace_original: true,
        text: "❌ The account linked to this channel doesn't have an active support entitlement. Please contact your account manager.",
      });
      break;

    case "contact_not_found":
      await replyToResponseUrl(response_url, {
        replace_original: true,
        text: "We couldn't find a Salesforce contact for your email address on this account.",
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: "We couldn't find a Salesforce contact for *" + email + "* on this account.\nPlease create your profile to request access.",
            },
          },
          {
            type: "actions",
            elements: [
              {
                type: "button",
                action_id: "barry_create_contact",
                text: { type: "plain_text", text: "Create Profile" },
                value: JSON.stringify({ team_id, user_id, email, channel_id, account_id: result.accountId }),
              },
            ],
          },
        ],
      });
      break;

    case "pending_approval":
      await replyToResponseUrl(response_url, {
        replace_original: true,
        text: "⏳ Your access request is pending approval. Barry will send you a direct message once it's approved.",
      });
      break;

    case "approved":
      await replyToResponseUrl(response_url, {
        replace_original: true,
        text: "✅ All checks passed.",
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: "✅ *All checks passed.* Click below to open the case form.",
            },
          },
          {
            type: "actions",
            elements: [
              {
                type: "button",
                action_id: "barry_open_case_form",
                text: { type: "plain_text", text: "Open Case Form" },
                style: "primary",
                value: JSON.stringify({
                  team_id,
                  user_id,
                  email,
                  channel_id,
                  account_id: result.accountId,
                  contact_id: result.contactId,
                }),
              },
            ],
          },
        ],
      });
      break;
  }
}

// ---- Slack command payload type (minimal) ----
type SlackCommandPayload = {
  team_id?: string;
  channel_id?: string;
  user_id?: string;
  command?: string;
  text?: string;
  response_url?: string;
  trigger_id?: string;
};

// ---- Create-case flow (returning verified user) ----
async function handleCreateCaseCommand(job: Job, payload: SlackCommandPayload) {
  const { response_url, team_id, channel_id, user_id } = payload;

  if (!response_url) throw new Error("Missing response_url in slack command payload");
  if (!team_id) throw new Error("Missing team_id in slack command payload");
  if (!channel_id) throw new Error("Missing channel_id in slack command payload");
  if (!user_id) throw new Error("Missing user_id in slack command payload");

  await replyToResponseUrl(response_url, {
    replace_original: true,
    text: "🔍 Verifying your access…",
  });

  // Get email from DB — user was pre-verified in vercel
  const { rows } = await pool.query(
    `SELECT email FROM slack_user_link WHERE slack_team_id = $1 AND slack_user_id = $2 LIMIT 1`,
    [team_id, user_id]
  );
  if (rows.length === 0) {
    await replyToResponseUrl(response_url, {
      replace_original: true,
      text: "⚠️ Your email isn't verified yet. Please run `/create-case` again to complete setup.",
    });
    return;
  }
  const email: string = rows[0].email;

  // Full Salesforce validation
  const result = await sfValidateUser(team_id, channel_id, email, user_id);
  await handleValidationResult(result, { team_id, user_id, email, channel_id, response_url });
}

// ---- Core Worker ----
const worker = new Worker(
  "barry-jobs",
  async (job: Job) => {
    const correlationId = job.data?.correlation_id || `job-${job.id}-${Date.now()}`;

    console.log("---- NEW JOB ----");
    console.log("Job ID:", job.id);
    console.log("Job name:", job.name);
    console.log("Attempts made:", job.attemptsMade);
    console.log("Correlation ID:", correlationId);
    console.log("Payload:", job.data);
    console.log("-----------------");

    // ---- Write "started" audit log ----
    await pool.query(
      `INSERT INTO audit_log (source, action, status, correlation_id, payload) VALUES ($1, $2, $3, $4, $5)`,
      ["queue", job.name, "started", correlationId, job.data || {}]
    );

    try {
      // 1) Slack interactivity jobs (legacy — button clicks that don't have a specific handler)
      if (job.name === "slack-interaction") {
        const interactionPayload = job.data?.payload as {
          channel?: { id?: string };
          message?: { ts?: string };
        };

        const channel = interactionPayload?.channel?.id;
        const ts      = interactionPayload?.message?.ts;

        if (!channel || !ts) {
          // Ephemeral/app-home interactions don't carry channel+ts — nothing to update, skip.
          console.log("[slack-interaction] no channel/ts in payload — skipping", job.data?.correlation_id);
          return;
        }

        const token = requireEnv("SLACK_BOT_TOKEN");
        await slackUpdateMessage(token, channel, ts, "✅ *Barry received your click (via worker)*");
      }

      // 2) Slash command jobs — /create-case for already-verified users
      if (job.name === "slack-command") {
        const cmdPayload = job.data?.payload as SlackCommandPayload | undefined;
        if (!cmdPayload) throw new Error("Missing payload on slack-command job");

        const cmd = cmdPayload.command || "";
        if (cmd === "/create-case" || cmd === "/raise-case") {
          await handleCreateCaseCommand(job, cmdPayload);
        }
      }

      // 3) Full SF validation — runs after first-time email capture
      if (job.name === "verify-user") {
        const { team_id, channel_id, user_id, email, response_url } = job.data as {
          team_id: string;
          channel_id: string;
          user_id: string;
          email: string;
          response_url: string;
        };

        if (!response_url) {
          console.warn("[verify-user] No response_url — cannot notify user");
          return;
        }

        await replyToResponseUrl(response_url, {
          replace_original: true,
          text: "🔍 Verifying your access…",
        });

        const result = await sfValidateUser(team_id, channel_id, email, user_id);
        await handleValidationResult(result, { team_id, user_id, email, channel_id, response_url });
      }

      // 4) Create Salesforce Contact for users not yet in the system
      if (job.name === "create-contact") {
        const {
          team_id, user_id, email, channel_id, account_id,
          first_name, last_name, phone, job_title, response_url,
        } = job.data as {
          team_id: string; user_id: string; email: string; channel_id: string;
          account_id: string; first_name: string; last_name: string;
          phone?: string; job_title?: string; response_url: string;
        };

        if (!response_url) {
          console.warn("[create-contact] No response_url — cannot notify user");
        }

        const sfRes = await sfCreateContact({
          accountId: account_id,
          email,
          firstName: first_name,
          lastName: last_name,
          phone,
          jobTitle: job_title,
          slackUserId: user_id,
          slackTeamId: team_id,
        });

        if (!sfRes.success) {
          console.error("[create-contact] SF create failed:", sfRes.error);
          if (response_url) {
            await replyToResponseUrl(response_url, {
              replace_original: true,
              text: `❌ We couldn't create your profile: ${sfRes.error ?? "unknown error"}. Please contact your account admin.`,
            });
          }
          return;
        }

        console.log(`[create-contact] Contact created: ${sfRes.contactId}`);

        if (response_url) {
          await replyToResponseUrl(response_url, {
            replace_original: true,
            text: "✅ *Profile submitted for approval.*\nBarry will send you a direct message once your access is approved.",
          });
        }
      }

      // 5) Case submission — creates SF Case
      if (job.name === "create-case") {
        const {
          account_id, contact_id, subject, description,
          priority, type, response_url, user_id, email, channel_id,
        } = job.data as {
          team_id: string; channel_id: string; user_id: string; email: string;
          account_id: string; contact_id: string;
          subject: string; description: string;
          priority: string; type: string;
          response_url: string;
        };

        console.log(`[create-case] user=${user_id} email=${email} subject="${subject}"`);

        if (!response_url) {
          console.warn("[create-case] No response_url — cannot notify user");
        }

        const sfRes = await sfCreateCase({
          accountId: account_id,
          contactId: contact_id,
          subject,
          description,
          priority: priority || "Medium",
          type: type || "Question",
        });

        if (!sfRes.success) {
          console.error("[create-case] SF create failed:", sfRes.error);
          if (response_url) {
            await replyToResponseUrl(response_url, {
              replace_original: true,
              text: `❌ We couldn't create your case: ${sfRes.error ?? "unknown error"}. Please try again or contact your account admin.`,
            });
          }
          return;
        }

        console.log(`[create-case] Case created: #${sfRes.caseNumber} (${sfRes.caseId})`);

        const priorityEmoji = (priority || "Medium") === "High" ? "🔴" : (priority || "Medium") === "Low" ? "🟢" : "🟡";

        // 1) Short private confirmation — replaces the ephemeral "Open Case Form" button
        if (response_url) {
          await replyToResponseUrl(response_url, {
            replace_original: true,
            text: `✅ Case *#${sfRes.caseNumber}* raised successfully.`,
          });
        }

        // 2) Public channel announcement — visible to everyone
        const token = requireEnv("SLACK_BOT_TOKEN");

        // Build blocks shared between initial post and the update-with-button
        const announcementBlocks = [
          {
            type: "header",
            text: { type: "plain_text", text: "🆕 New Support Case Raised" },
          },
          {
            type: "section",
            fields: [
              { type: "mrkdwn", text: `*Case Number*\n\`#${sfRes.caseNumber}\`` },
              { type: "mrkdwn", text: `*Priority*\n${priorityEmoji} ${priority || "Medium"}` },
              { type: "mrkdwn", text: `*Type*\n${type || "Question"}` },
              { type: "mrkdwn", text: `*Raised By*\n<@${user_id}>` },
            ],
          },
          {
            type: "section",
            text: { type: "mrkdwn", text: `*Subject*\n${subject}` },
          },
          ...(description ? [{
            type: "section" as const,
            text: { type: "mrkdwn", text: `*Description*\n${description}` },
          }] : []),
          { type: "divider" as const },
          {
            type: "context",
            elements: [
              {
                type: "mrkdwn",
                text: `<!channel> A new case has been raised and logged in Salesforce. Our team will be in touch.`,
              },
              {
                type: "mrkdwn",
                text: `💬 *Reply in this thread* to add information or discuss the case — your replies are automatically logged to Salesforce.`,
              },
            ],
          },
        ];

        const msgRes = await fetch("https://slack.com/api/chat.postMessage", {
          method: "POST",
          headers: {
            "Content-Type": "application/json; charset=utf-8",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            channel: channel_id,
            text: `New support case raised: #${sfRes.caseNumber} — ${subject}`,
            blocks: announcementBlocks,
          }),
        });

        const msgBody = await msgRes.json() as { ok: boolean; ts?: string };

        if (msgBody.ok && msgBody.ts && sfRes.caseId) {
          // Save the Case → Slack message mapping for status updates, comments and close
          await pool.query(
            `INSERT INTO case_slack_link (case_id, case_number, channel_id, message_ts)
             VALUES ($1, $2, $3, $4) ON CONFLICT (case_id) DO NOTHING`,
            [sfRes.caseId, sfRes.caseNumber, channel_id, msgBody.ts]
          );

          // Write Slack routing fields back to Salesforce so the Conversation Centre
          // knows this case lives in Slack and can route replies correctly.
          try {
            await salesforce.sfFetch(
              salesforce.sfRestPath(`/sobjects/Case/${sfRes.caseId}`),
              {
                method: "PATCH",
                body: JSON.stringify({
                  Barry_Enabled__c:    true,
                  Slack_Thread_Ts__c:  msgBody.ts,
                  Slack_Channel_Id__c: channel_id,
                  Origin_Channel__c:   "Slack",
                }),
              }
            );
            console.log(`[create-case] Slack routing fields written to Case ${sfRes.caseId}`);
          } catch (patchErr) {
            // Non-fatal — case still created and thread mapped; log and continue
            console.error("[create-case] Failed to patch Slack routing fields:", patchErr);
          }

          // Add "Mark as Resolved" button via chat.update
          await fetch("https://slack.com/api/chat.update", {
            method: "POST",
            headers: {
              "Content-Type": "application/json; charset=utf-8",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              channel: channel_id,
              ts: msgBody.ts,
              blocks: [
                ...announcementBlocks,
                {
                  type: "actions",
                  elements: [
                    {
                      type: "button",
                      action_id: "barry_close_case",
                      text: { type: "plain_text", text: "Mark as Resolved ✓" },
                      style: "primary",
                      confirm: {
                        title: { type: "plain_text", text: "Close this case?" },
                        text: {
                          type: "mrkdwn",
                          text: `This will mark *Case #${sfRes.caseNumber}* as Closed in Salesforce.`,
                        },
                        confirm: { type: "plain_text", text: "Yes, close it" },
                        deny: { type: "plain_text", text: "Cancel" },
                      },
                      value: JSON.stringify({
                        case_id: sfRes.caseId,
                        case_number: sfRes.caseNumber,
                        channel_id,
                      }),
                    },
                  ],
                },
              ],
            }),
          });
        }
      }

      // 6) Add case comment — Slack thread reply → SF CaseComment
      if (job.name === "add-case-comment") {
        const { case_id, comment_body, author_name } = job.data as {
          case_id: string; case_number: string; comment_body: string; author_name: string;
        };

        await salesforce.sfJson("/services/apexrest/barry/add-case-comment", {
          method: "POST",
          body: JSON.stringify({ caseId: case_id, body: comment_body, authorName: author_name }),
          headers: { "Content-Type": "application/json; charset=utf-8" },
        });

        console.log(`[add-case-comment] Comment added to SF case ${case_id}`);
      }

      // 7) CSAT response — user clicked a rating button on the survey DM
      if (job.name === "csat-response") {
        const { case_id, case_number, rating, response_url } = job.data as {
          case_id: string; case_number: string; rating: number; response_url: string;
        };

        const sfRes = await sfSaveCsat(case_id, rating);

        if (!sfRes.success) {
          console.error("[csat-response] SF save failed:", sfRes.error, { case_id, rating });
        } else {
          console.log(`[csat-response] CSAT score ${rating}/5 saved for case ${case_id} (#${case_number})`);
        }

        // Update the DM — show thank-you + optional feedback prompt
        if (response_url) {
          const stars = "⭐".repeat(rating);
          await replyToResponseUrl(response_url, {
            replace_original: true,
            text: `Thanks for rating Case #${case_number} — ${stars} (${rating}/5).`,
            blocks: [
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text: `${stars} *Thanks for rating Case #${case_number}* (${rating}/5) — your score has been recorded.`,
                },
              },
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text: "Want to tell us a bit more about your experience? It only takes a moment and really helps us improve.",
                },
              },
              {
                type: "actions",
                elements: [
                  {
                    type: "button",
                    action_id: "barry_csat_feedback_open",
                    style: "primary",
                    text: { type: "plain_text", text: "Tell us more 💬" },
                    value: JSON.stringify({ case_id, case_number, rating }),
                  },
                ],
              },
            ],
          });
        }
      }

      // 7b) csat-feedback — save optional written feedback to CSAT_Feedback__c
      if (job.name === "csat-feedback") {
        const { case_id, case_number, feedback } = job.data as {
          case_id: string; case_number: string; feedback: string;
        };
        await sfSaveCsatFeedback(case_id, feedback);
        console.log(`[csat-feedback] Feedback saved for case ${case_id} (#${case_number})`);
      }

      // 8) Get cases — /view-cases command and pagination buttons
      if (job.name === "get-cases") {
        const { channel_id, user_id, team_id, response_url, page = 0, filter = "open" } = job.data as {
          channel_id: string; user_id: string; team_id?: string;
          response_url: string; page: number; filter?: string;
          account_id?: string; // present on pagination/filter button clicks
        };

        const token = requireEnv("SLACK_BOT_TOKEN");

        // Pagination buttons already carry account_id — skip SF channel lookup
        let accountId = job.data.account_id as string | undefined;

        if (!accountId) {
          // Fresh /view-cases command — need to resolve accountId from channel
          const { rows: emailRows } = await pool.query<{ email: string }>(
            "SELECT email FROM slack_user_link WHERE slack_team_id = $1 AND slack_user_id = $2 LIMIT 1",
            [team_id || "", user_id]
          );
          const email = emailRows[0]?.email || "";
          const validation = await sfValidateUser(team_id || "", channel_id, email, user_id);

          if (validation.status === "channel_not_linked") {
            await replyToResponseUrl(response_url, {
              replace_original: true,
              text: "❌ This channel isn't connected to a Salesforce account yet.",
            });
            return;
          }
          accountId = validation.accountId;
        }

        if (!accountId) {
          await replyToResponseUrl(response_url, { replace_original: true, text: "❌ Could not determine account for this channel." });
          return;
        }

        const sfRes = await sfGetCases(accountId);

        if (!sfRes.success || !sfRes.cases) {
          await replyToResponseUrl(response_url, { replace_original: true, text: `❌ Could not fetch cases: ${sfRes.error ?? "unknown error"}` });
          return;
        }

        // Filter by open/closed, then sort by custom status order
        const isClosed = filter === "closed";
        const filtered = sfRes.cases.filter(c =>
          isClosed ? c.status === "Closed" : c.status !== "Closed"
        );
        const sorted = [...filtered].sort((a, b) => {
          const orderA = STATUS_ORDER[a.status] ?? 99;
          const orderB = STATUS_ORDER[b.status] ?? 99;
          if (orderA !== orderB) return orderA - orderB;
          return new Date(b.createdDate).getTime() - new Date(a.createdDate).getTime();
        });

        const total = sorted.length;
        const totalPages = Math.max(1, Math.ceil(total / CASES_PAGE_SIZE));
        const safePage = Math.min(Math.max(0, page), totalPages - 1);
        const slice = sorted.slice(safePage * CASES_PAGE_SIZE, (safePage + 1) * CASES_PAGE_SIZE);

        // Build blocks
        const filterValue = (f: string) => JSON.stringify({ account_id: accountId, channel_id, filter: f, page: 0, response_url });
        const blocks: unknown[] = [
          {
            type: "header",
            text: { type: "plain_text", text: "📋 Account Cases" },
          },
          {
            type: "actions",
            elements: [
              {
                type: "button",
                action_id: "barry_cases_filter_open",
                text: { type: "plain_text", text: "📂 Open" },
                ...(isClosed ? {} : { style: "primary" }),
                value: filterValue("open"),
              },
              {
                type: "button",
                action_id: "barry_cases_filter_closed",
                text: { type: "plain_text", text: "🤝 Closed" },
                ...(isClosed ? { style: "primary" } : {}),
                value: filterValue("closed"),
              },
            ],
          },
          {
            type: "context",
            elements: [{
              type: "mrkdwn",
              text: total === 0
                ? `No ${isClosed ? "closed" : "open"} cases found for this account.`
                : `Showing *${safePage * CASES_PAGE_SIZE + 1}–${safePage * CASES_PAGE_SIZE + slice.length}* of *${total}* ${isClosed ? "closed" : "open"} cases · Page ${safePage + 1} of ${totalPages}`,
            }],
          },
          { type: "divider" },
        ];

        for (const c of slice) {
          const statusEmoji = STATUS_EMOJI[c.status] ?? "❓";
          const priorityEmoji = c.priority === "High" ? "🔴" : c.priority === "Low" ? "🟢" : "🟡";
          const date = new Date(c.createdDate).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
          const raisedBy = c.contactName ? ` · Raised by ${c.contactName}` : "";
          blocks.push({
            type: "section",
            text: {
              type: "mrkdwn",
              text:
                `*#${c.caseNumber}* · ${statusEmoji} ${c.status} · ${priorityEmoji} ${c.priority}\n` +
                `_${c.subject}_` +
                (c.type ? ` · ${c.type}` : "") +
                `\n_Opened ${date}${raisedBy}_`,
            },
          });
          blocks.push({ type: "divider" });
        }

        // Pagination nav
        if (totalPages > 1) {
          const navValue = JSON.stringify({ account_id: accountId, channel_id, filter, page: safePage, response_url });
          const navElements: unknown[] = [];
          if (safePage > 0) {
            navElements.push({ type: "button", action_id: "barry_cases_prev", text: { type: "plain_text", text: "← Previous" }, value: navValue });
          }
          if (safePage < totalPages - 1) {
            navElements.push({ type: "button", action_id: "barry_cases_next", style: "primary", text: { type: "plain_text", text: "Next →" }, value: navValue });
          }
          if (navElements.length > 0) blocks.push({ type: "actions", elements: navElements });
        }

        await replyToResponseUrl(response_url, {
          replace_original: true,
          text: `Your cases — page ${safePage + 1} of ${totalPages}`,
          blocks,
        });

        console.log(`[get-cases] Sent page ${safePage + 1}/${totalPages} (${slice.length} cases) to ${user_id}`);
      }

      // 8) Close case — called when user clicks "Mark as Resolved" in Slack
      if (job.name === "close-case") {
        const { case_id, case_number, channel_id, user_id } = job.data as {
          case_id: string; case_number: string; channel_id: string; user_id: string;
        };

        const sfRes = await sfCloseCase(case_id);

        if (!sfRes.success) {
          console.error("[close-case] SF close failed:", sfRes.error);
          return;
        }

        // Look up the original announcement message_ts
        const { rows } = await pool.query<{ message_ts: string }>(
          "SELECT message_ts FROM case_slack_link WHERE case_id = $1",
          [case_id]
        );

        const token = requireEnv("SLACK_BOT_TOKEN");

        if (rows.length > 0) {
          const message_ts = rows[0].message_ts;

          // Update announcement: replace "Mark as Resolved" button with a CLOSED badge
          await fetch("https://slack.com/api/chat.update", {
            method: "POST",
            headers: {
              "Content-Type": "application/json; charset=utf-8",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              channel: channel_id,
              ts: message_ts,
              blocks: [
                {
                  type: "header",
                  text: { type: "plain_text", text: "✅ Support Case Closed" },
                },
                {
                  type: "section",
                  text: {
                    type: "mrkdwn",
                    text: `*Case #${case_number}* has been marked as resolved by <@${user_id}>.`,
                  },
                },
              ],
            }),
          });

          // Thread reply confirming closure
          await fetch("https://slack.com/api/chat.postMessage", {
            method: "POST",
            headers: {
              "Content-Type": "application/json; charset=utf-8",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              channel: channel_id,
              thread_ts: message_ts,
              text: `✅ Case #${case_number} marked as resolved by <@${user_id}>.`,
            }),
          });
        }

        console.log(`[close-case] Case ${case_id} (#${case_number}) closed by ${user_id}`);
      }

      // ---- Write "completed" audit log ----
      await pool.query(
        `INSERT INTO audit_log (source, action, status, correlation_id, payload) VALUES ($1, $2, $3, $4, $5)`,
        ["queue", job.name, "completed", correlationId, job.data || {}]
      );

      console.log(`✅ Job ${job.id} completed`);
      return { ok: true, processedAt: new Date().toISOString() };
    } catch (err: unknown) {
      console.error(`❌ Job ${job.id} failed`, getErrorMessage(err));

      // ---- Write "failed" audit log ----
      await pool.query(
        `INSERT INTO audit_log (source, action, status, correlation_id, payload) VALUES ($1, $2, $3, $4, $5)`,
        [
          "queue",
          job.name,
          "failed",
          correlationId,
          { error: getErrorMessage(err), originalPayload: job.data || {} },
        ]
      );

      throw err; // important so BullMQ retries
    }
  },
  { connection, concurrency: 5 }
);

// ---- Worker Lifecycle Events ----
worker.on("ready", () => console.log("✅ Worker ready"));
worker.on("error", (err) => console.error("❌ Worker error:", getErrorMessage(err)));
worker.on("failed", (job, err) => console.error(`❌ Job ${job?.id} permanently failed:`, getErrorMessage(err)));

// ---- Graceful shutdown ----
process.on("SIGINT", async () => {
  console.log("Shutting down worker...");
  await worker.close();
  await pool.end();
  process.exit(0);
});
