import "dotenv/config";
import { pool } from "./db";

async function main() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id bigserial PRIMARY KEY,
      created_at timestamptz NOT NULL DEFAULT now(),
      source text NOT NULL,
      action text NOT NULL,
      status text NOT NULL,
      correlation_id text,
      payload jsonb
    );
  `);

  console.log("✅ audit_log table is ready");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS case_slack_link (
      case_id     TEXT PRIMARY KEY,
      case_number TEXT NOT NULL,
      channel_id  TEXT NOT NULL,
      message_ts  TEXT NOT NULL,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  console.log("✅ case_slack_link table is ready");
  await pool.end();
}

main().catch(async (err) => {
  console.error("❌ migration failed:", err);
  await pool.end();
  process.exit(1);
});