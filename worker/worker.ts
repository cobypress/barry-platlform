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


// ---- Salesforce helpers ----
type SfChannelLookupResponse = {
  channelLinked: boolean;
  accountId?: string | null;
};

async function sfChannelLookup(
  slackTeamId: string,
  slackChannelId: string
): Promise<SfChannelLookupResponse> {
  const res = await salesforce.sfJson<SfChannelLookupResponse>("/services/apexrest/barry/channel", {
    method: "POST",
    body: JSON.stringify({ slackTeamId, slackChannelId }),
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });

  return res;
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

// ---- Create-case flow (verified returning user) ----
async function handleCreateCaseCommand(job: Job, payload: SlackCommandPayload) {
  const response_url = payload.response_url;
  const team_id = payload.team_id;
  const channel_id = payload.channel_id;
  const user_id = payload.user_id;

  if (!response_url) throw new Error("Missing response_url in slack command payload");
  if (!team_id) throw new Error("Missing team_id in slack command payload");
  if (!channel_id) throw new Error("Missing channel_id in slack command payload");
  if (!user_id) throw new Error("Missing user_id in slack command payload");

  await replyToResponseUrl(response_url, {
    response_type: "ephemeral",
    text: "🔍 Verifying your access…",
  });

  // 1) Get email from DB — user was pre-verified in vercel
  const { rows } = await pool.query(
    `SELECT email FROM slack_user_link WHERE slack_team_id = $1 AND slack_user_id = $2 LIMIT 1`,
    [team_id, user_id]
  );
  if (rows.length === 0) {
    await replyToResponseUrl(response_url, {
      response_type: "ephemeral",
      text: "⚠️ Your email isn’t verified yet. Please run `/create-case` again to complete setup.",
    });
    return;
  }
  const email: string = rows[0].email;

  // 2) Channel → Salesforce Account link
  const channelRes = await sfChannelLookup(team_id, channel_id);
  if (!channelRes.channelLinked) {
    await replyToResponseUrl(response_url, {
      response_type: "ephemeral",
      text:
        "❌ This Slack channel isn’t linked to a customer account yet.\n" +
        "Please ask your Account Owner to link it.",
    });
    return;
  }

  // TODO Phase 2: SF Contact lookup (verify email maps to a Contact in this account)
  // TODO Phase 2: SF Entitlement check (account has an active entitlement)

  // All checks pass — send "Open Case Form" button
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
            value: JSON.stringify({ team_id, user_id, email, channel_id }),
          },
        ],
      },
    ],
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

      // 3) Case submission jobs (from barry_case_intake modal)
      if (job.name === "create-case") {
        const { team_id, channel_id, user_id, email, subject, description, response_url } =
          job.data as {
            team_id: string;
            channel_id: string;
            user_id: string;
            email: string;
            subject: string;
            description: string;
            response_url: string;
          };

        console.log(`[create-case] user=${user_id} email=${email} subject="${subject}"`);

        if (!response_url) {
          console.warn("[create-case] No response_url — cannot notify user");
        }

        // Channel → Salesforce Account link
        const channelRes = await sfChannelLookup(team_id, channel_id);
        if (!channelRes.channelLinked) {
          if (response_url) {
            await replyToResponseUrl(response_url, {
              response_type: "ephemeral",
              text: "❌ This Slack channel isn't linked to a customer account. Case not created.",
            });
          }
          return;
        }

        // TODO Phase 2: SF Contact lookup (email → contactId in account)
        // TODO Phase 2: SF Entitlement check (active entitlement on account)
        // TODO Phase 2: create SF Case via sfFetch

        if (response_url) {
          await replyToResponseUrl(response_url, {
            response_type: "ephemeral",
            text:
              `✅ *Case received* — Salesforce submission coming in Phase 2.\n` +
              `• Subject: *${subject}*\n` +
              `• Account: *${channelRes.accountId}*`,
          });
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