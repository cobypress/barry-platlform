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
  await pool.end();
}

main().catch(async (err) => {
  console.error("❌ migration failed:", err);
  await pool.end();
  process.exit(1);
});