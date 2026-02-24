import "dotenv/config";
import { Worker } from "bullmq";
import { URL } from "node:url";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name} in environment variables`);
  return v;
}

const redisUrl = requireEnv("REDIS_URL");

// Helpful debug (won't reveal password)
try {
  const u = new URL(redisUrl);
  console.log("Redis:", {
    protocol: u.protocol,
    host: u.hostname,
    port: u.port,
    hasPassword: Boolean(u.password),
  });
} catch {
  console.log("REDIS_URL is not a valid URL format.");
}

const u = new URL(redisUrl);

// BullMQ/ioredis connection config (more reliable than passing url directly)
const connection: any = {
  host: u.hostname,
  port: u.port ? Number(u.port) : 6379,
  password: u.password || undefined,
};

// Upstash usually uses TLS ("rediss://")
if (u.protocol === "rediss:") {
  connection.tls = {}; // ioredis uses presence of tls:{} to enable TLS
}

console.log("Worker starting... listening on queue: barry-jobs");

const worker = new Worker(
  "barry-jobs",
  async (job) => {
    console.log("---- NEW JOB ----");
    console.log("Job ID:", job.id);
    console.log("Job name:", job.name);
    console.log("Attempts made:", job.attemptsMade);
    console.log("Payload:", job.data);
    console.log("-----------------");

    // Put real work here later (Slack/Salesforce calls).
    // For now, just prove processing works.
    return { ok: true, processedAt: new Date().toISOString() };
  },
  {
    connection,
    // You can tune concurrency later. Keep it low at first.
    concurrency: 5,
  }
);

worker.on("ready", () => console.log("✅ Worker ready"));
worker.on("completed", (job) => console.log(`✅ Job ${job.id} completed`));
worker.on("failed", (job, err) => {
  console.error(`❌ Job ${job?.id} failed:`, err?.message || err);
});
worker.on("error", (err) => {
  console.error("❌ Worker error:", err?.message || err);
});

// Keep process alive
process.on("SIGINT", async () => {
  console.log("Shutting down worker...");
  await worker.close();
  process.exit(0);
});