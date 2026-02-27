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
    case "channel_not_linked":
      await replyToResponseUrl(response_url, {
        response_type: "ephemeral",
        text: "❌ This Slack channel isn't linked to a customer account yet. Please ask your Account Owner to set it up.",
      });
      break;

    case "no_entitlement":
      await replyToResponseUrl(response_url, {
        response_type: "ephemeral",
        text: "❌ The account linked to this channel doesn't have an active support entitlement. Please contact your account manager.",
      });
      break;

    case "contact_not_found":
      await replyToResponseUrl(response_url, {
        response_type: "ephemeral",
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
        response_type: "ephemeral",
        text: "⏳ Your access request is pending approval. Barry will send you a direct message once it's approved.",
      });
      break;

    case "approved":
      await replyToResponseUrl(response_url, {
        response_type: "ephemeral",
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
    response_type: "ephemeral",
    text: "🔍 Verifying your access…",
  });

  // Get email from DB — user was pre-verified in vercel
  const { rows } = await pool.query(
    `SELECT email FROM slack_user_link WHERE slack_team_id = $1 AND slack_user_id = $2 LIMIT 1`,
    [team_id, user_id]
  );
  if (rows.length === 0) {
    await replyToResponseUrl(response_url, {
      response_type: "ephemeral",
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
        const token = requireEnv("SLACK_BOT_TOKEN");

        const interactionPayload = job.data?.payload as {
          channel?: { id?: string };
          message?: { ts?: string };
        };

        const channel = interactionPayload?.channel?.id;
        const ts = interactionPayload?.message?.ts;

        if (!channel || !ts) {
          throw new Error("Missing channel.id or message.ts in interaction payload");
        }

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
          response_type: "ephemeral",
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
              response_type: "ephemeral",
              text: `❌ We couldn't create your profile: ${sfRes.error ?? "unknown error"}. Please contact your account admin.`,
            });
          }
          return;
        }

        console.log(`[create-contact] Contact created: ${sfRes.contactId}`);

        if (response_url) {
          await replyToResponseUrl(response_url, {
            response_type: "ephemeral",
            text: "✅ *Profile submitted for approval.*\nBarry will send you a direct message once your access is approved.",
          });
        }
      }

      // 5) Case submission — creates SF Case (Phase 2)
      if (job.name === "create-case") {
        const {
          team_id, channel_id, user_id, email, account_id, contact_id,
          subject, description, response_url,
        } = job.data as {
          team_id: string; channel_id: string; user_id: string; email: string;
          account_id?: string; contact_id?: string;
          subject: string; description: string; response_url: string;
        };

        console.log(`[create-case] user=${user_id} email=${email} subject="${subject}"`);

        if (!response_url) {
          console.warn("[create-case] No response_url — cannot notify user");
        }

        // TODO Phase 2: create SF Case via sfFetch using account_id, contact_id, subject, description

        if (response_url) {
          await replyToResponseUrl(response_url, {
            response_type: "ephemeral",
            text:
              `✅ *Case received* — Salesforce submission coming in Phase 2.\n` +
              `• Subject: *${subject}*`,
          });
        }
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
