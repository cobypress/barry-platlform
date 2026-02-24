import "dotenv/config";
import { Queue } from "bullmq";
import { URL } from "node:url";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name} in environment variables`);
  return v;
}

const redisUrl = requireEnv("REDIS_URL");
const u = new URL(redisUrl);

const connection: any = {
  host: u.hostname,
  port: u.port ? Number(u.port) : 6379,
  password: u.password || undefined,
};

if (u.protocol === "rediss:") {
  connection.tls = {};
}

async function main() {
  const queue = new Queue("barry-jobs", { connection });

  const job = await queue.add(
    "test",
    {
      message: "Hello from send-job.ts",
      when: new Date().toISOString(),
    },
    {
      // sensible defaults for now
      attempts: 3,
      backoff: { type: "exponential", delay: 2000 },
      removeOnComplete: true,
      removeOnFail: false,
    }
  );

  console.log("✅ Enqueued job:", job.id);
  await queue.close();
}

main().catch((err) => {
  console.error("❌ Failed to enqueue job:", err?.message || err);
  process.exit(1);
});