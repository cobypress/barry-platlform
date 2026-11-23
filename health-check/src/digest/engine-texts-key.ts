import { createHash } from "node:crypto";

// The key a text is known by among the engine's own (DECISIONS.md 693): a
// short hash of the whole text, exactly as the source writes it.

export function engineTextKey(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex").slice(0, 16);
}
