import "dotenv/config";
import { Worker, Job } from "bullmq";
import { URL } from "node:url";
import { pool } from "./db";

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
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text },
        },
      ],
    }),
  });

  const data = (await res.json()) as { ok: boolean; error?: string };
  if (!data.ok) throw new Error(`Slack chat.update failed: ${data.error}`);
}


type SlackAuthTestResponse = {
  ok: boolean;
  team_id?: string;
  user_id?: string;
  error?: string;
};

type SlackUsersInfoResponse = {
  ok: boolean;
  error?: string;
  user?: { profile?: { email?: string } };
};

/**
 * Tries to retrieve a Slack user's email.
 * Returns null if Slack won't provide it (common causes: missing scope, external users, user_not_found).
 */
async function slackTryGetUserEmail(token: string, userId: string): Promise<string | null> {
  // Optional debug: verify token belongs to the workspace you expect
  if (process.env.SLACK_DEBUG_AUTH_TEST === "true") {
    const authRes = await fetch("https://slack.com/api/auth.test", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json; charset=utf-8",
      },
    });

    const authData = (await authRes.json()) as SlackAuthTestResponse;
    console.log("SLACK auth.test:", authData);
  }

  const res = await fetch("https://slack.com/api/users.info", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json; charset=utf-8",
    },
    body: JSON.stringify({ user: userId }),
  });

  const data = (await res.json()) as SlackUsersInfoResponse;

  if (!data.ok) {
    // Common non-fatal cases: you can’t read email, token/workspace mismatch, etc.
    console.warn("Slack users.info failed:", { userId, error: data.error });

    // If you want to be stricter for internal users later, you can throw here instead.
    return null;
  }

  return data.user?.profile?.email || null;
}

// ---- Salesforce helpers ----
type SfChannelLookupResponse = {
  channelLinked: boolean;
  accountId?: string | null;
};

async function sfChannelLookup(slackTeamId: string, slackChannelId: string): Promise<SfChannelLookupResponse> {
  const instanceUrl = requireEnv("SF_INSTANCE_URL");
  const accessToken = requireEnv("SF_ACCESS_TOKEN");

  const res = await fetch(`${instanceUrl}/services/apexrest/barry/channel`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json; charset=utf-8",
    },
    body: JSON.stringify({ slackTeamId, slackChannelId }),
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`SF channel lookup failed: ${res.status} ${text}`);


  // Salesforce sometimes returns empty body on some failures; be defensive
  const data = JSON.parse(text) as SfChannelLookupResponse;
  return data;
}

async function slackOpenEmailModal(token: string, triggerId: string) {
  const res = await fetch("https://slack.com/api/views.open", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json; charset=utf-8",
    },
    body: JSON.stringify({
      trigger_id: triggerId,
      view: {
        type: "modal",
        callback_id: "barry_email_capture",
        title: { type: "plain_text", text: "Verify your email" },
        submit: { type: "plain_text", text: "Verify" },
        close: { type: "plain_text", text: "Cancel" },
        blocks: [
          {
            type: "input",
            block_id: "email_block",
            label: { type: "plain_text", text: "Work email" },
            element: {
              type: "plain_text_input",
              action_id: "email_input",
              placeholder: { type: "plain_text", text: "name@company.com" },
            },
          },
        ],
      },
    }),
  });

  const data = (await res.json()) as { ok: boolean; error?: string };
  if (!data.ok) throw new Error(`Slack views.open failed: ${data.error}`);
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

// ---- Create-case flow (Phase 1) ----
async function handleCreateCaseCommand(job: Job, payload: SlackCommandPayload) {
  const token = requireEnv("SLACK_BOT_TOKEN");

  const responseUrl = payload.response_url;
  const teamId = payload.team_id;
  const channelId = payload.channel_id;
  const userId = payload.user_id;

  if (!responseUrl) throw new Error("Missing response_url in slack command payload");
  if (!teamId) throw new Error("Missing team_id in slack command payload");
  if (!channelId) throw new Error("Missing channel_id in slack command payload");
  if (!userId) throw new Error("Missing user_id in slack command payload");

  // Tell user we're checking (quick feedback)
  await replyToResponseUrl(responseUrl, {
    response_type: "ephemeral",
    text: "🔍 Checking this channel is linked to your account…",
  });

  // 1) Channel → Account link
  const channelRes = await sfChannelLookup(teamId, channelId);
  if (!channelRes.channelLinked) {
    await replyToResponseUrl(responseUrl, {
      response_type: "ephemeral",
      text:
        "❌ This Slack channel isn’t linked to a customer account yet.\n" +
        "Please ask your Account Owner to link it (internal command).",
    });
    return;
  }

  await replyToResponseUrl(responseUrl, {
    response_type: "ephemeral",
    text: "✅ Channel linked. Verifying your identity…",
  });

  // 2) Slack user email
  const email = await slackTryGetUserEmail(token, userId);

    if (!email) {
      if (!payload.trigger_id) throw new Error("Missing trigger_id (needed to open modal)");

      await replyToResponseUrl(responseUrl, {
        response_type: "ephemeral",
        text: "I couldn’t automatically read your email (common in Slack Connect). Opening verification…",
      });

      await slackOpenEmailModal(token, payload.trigger_id);
      return;
    }

  // Phase 1 success output (for now)
  await replyToResponseUrl(responseUrl, {
    response_type: "ephemeral",
    text:
      "✅ Identity found.\n" +
      `• Email: *${email}*\n` +
      `• AccountId (from channel link): *${channelRes.accountId}*\n\n` +
      "Next: I’ll check your Salesforce Contact, approval status, and entitlement, then open the case intake form.",
  });
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
      `
      INSERT INTO audit_log (source, action, status, correlation_id, payload)
      VALUES ($1, $2, $3, $4, $5)
      `,
      ["queue", job.name, "started", correlationId, job.data || {}]
    );

    try {
      // 1) Slack interactivity jobs (already working)
      if (job.name === "slack-interaction") {
        const token = requireEnv("SLACK_BOT_TOKEN");

        const payload = job.data?.payload as {
          channel?: { id?: string };
          message?: { ts?: string };
        };

        const channel = payload?.channel?.id;
        const ts = payload?.message?.ts;

        if (!channel || !ts) {
          throw new Error("Missing channel.id or message.ts in interaction payload");
        }

        await slackUpdateMessage(token, channel, ts, "✅ *Barry received your click (via worker)*");
      }

      // 2) Slash command jobs
      if (job.name === "slack-command") {
        const payload = job.data?.payload as SlackCommandPayload | undefined;
        if (!payload) throw new Error("Missing payload on slack-command job");

        const cmd = payload.command || "";

        // Support both names for now:
        // /create-case is your preferred command
        // /raise-case can be an alias
        if (cmd === "/create-case" || cmd === "/raise-case") {
          await handleCreateCaseCommand(job, payload);
        }
      }

      // ---- Write "completed" audit log ----
      await pool.query(
        `
        INSERT INTO audit_log (source, action, status, correlation_id, payload)
        VALUES ($1, $2, $3, $4, $5)
        `,
        ["queue", job.name, "completed", correlationId, job.data || {}]
      );

      console.log(`✅ Job ${job.id} completed`);
      return { ok: true, processedAt: new Date().toISOString() };
    } catch (err: unknown) {
      console.error(`❌ Job ${job.id} failed`, getErrorMessage(err));

      // ---- Write "failed" audit log ----
      await pool.query(
        `
        INSERT INTO audit_log (source, action, status, correlation_id, payload)
        VALUES ($1, $2, $3, $4, $5)
        `,
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