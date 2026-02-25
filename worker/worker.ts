import "dotenv/config";
import { Worker, Job } from "bullmq";
import { URL } from "node:url";
import { pool } from "./db";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name} in environment variables`);
  return v;
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

// ---- Core Worker ----
const worker = new Worker(
  "barry-jobs",
  async (job: Job) => {
    const correlationId =
      job.data?.correlation_id || `job-${job.id}-${Date.now()}`;

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
      [
        "queue",
        job.name,
        "started",
        correlationId,
        job.data || {},
      ]
    );

    try {
      // ======================================
      // 🔥 REAL WORK GOES HERE LATER
      // Slack calls
      // Salesforce calls
      // Routing logic
      // ======================================

      // Simulate small processing delay
      if (job.name === "slack-interaction") {
        const token = process.env.SLACK_BOT_TOKEN;
        if (!token) throw new Error("Missing SLACK_BOT_TOKEN in worker env");

        const payload = job.data?.payload as {
          channel?: { id?: string };
          message?: { ts?: string };
        };

        const channel = payload?.channel?.id;
        const ts = payload?.message?.ts;

        if (!channel || !ts) {
          throw new Error("Missing channel.id or message.ts in interaction payload");
        }

        const res = await fetch("https://slack.com/api/chat.update", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json; charset=utf-8",
          },
          body: JSON.stringify({
            channel,
            ts,
            text: "✅ Barry received your click (via worker)",
            blocks: [
              {
                type: "section",
                text: { type: "mrkdwn", text: "✅ *Barry received your click (via worker)*" },
              },
            ],
          }),
        });

        const data = (await res.json()) as { ok: boolean; error?: string };
        if (!data.ok) throw new Error(`Slack chat.update failed: ${data.error}`);
      }

      // ---- Write "completed" audit log ----
      await pool.query(
        `
        INSERT INTO audit_log (source, action, status, correlation_id, payload)
        VALUES ($1, $2, $3, $4, $5)
        `,
        [
          "queue",
          job.name,
          "completed",
          correlationId,
          job.data || {},
        ]
      );

      console.log(`✅ Job ${job.id} completed`);

      return { ok: true, processedAt: new Date().toISOString() };
    } catch (error: any) {
      console.error(`❌ Job ${job.id} failed`, error?.message || error);

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
          {
            error: error?.message || "unknown error",
            originalPayload: job.data || {},
          },
        ]
      );

      throw error; // important so BullMQ retries
    }
  },
  {
    connection,
    concurrency: 5,
  }
);

// ---- Worker Lifecycle Events ----
worker.on("ready", () => console.log("✅ Worker ready"));
worker.on("error", (err) =>
  console.error("❌ Worker error:", err?.message || err)
);

worker.on("failed", (job, err) => {
  console.error(`❌ Job ${job?.id} permanently failed:`, err?.message || err);
});

// ---- Graceful shutdown ----
process.on("SIGINT", async () => {
  console.log("Shutting down worker...");
  await worker.close();
  await pool.end();
  process.exit(0);
});