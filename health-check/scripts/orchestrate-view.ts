// Reads a Claude Code stream-json session on stdin and prints one line per
// step, so the unattended loop's terminal stays readable. The raw stream is
// kept by orchestrate.sh; this only summarises it. Lines it doesn't recognise
// are skipped, never fatal.
import { createInterface } from "node:readline";

type Block = { type?: string; name?: string; text?: string; input?: Record<string, unknown> };
type Event = { type?: string; subtype?: string; message?: { content?: Block[] }; parent_tool_use_id?: string | null; result?: string; total_cost_usd?: number; num_turns?: number };

const time = (): string => new Date().toTimeString().slice(0, 8);
const short = (text: string, n = 110): string => {
  const one = text.replace(/\s+/g, " ").trim();
  return one.length > n ? `${one.slice(0, n - 1)}…` : one;
};

function describe(block: Block): string | null {
  if (block.type === "text" && block.text?.trim()) return `says: ${short(block.text)}`;
  if (block.type !== "tool_use") return null;
  const input = block.input ?? {};
  if (block.name === "Agent" || block.name === "Task") return `starts ${String(input.subagent_type ?? "agent")}: ${short(String(input.description ?? ""), 80)}`;
  if (block.name === "Bash") return `runs: ${short(String(input.command ?? ""), 90)}`;
  const path = input.file_path ?? input.path ?? input.pattern;
  return `${block.name}${path ? `: ${short(String(path), 90)}` : ""}`;
}

for await (const line of createInterface({ input: process.stdin })) {
  let event: Event;
  try {
    event = JSON.parse(line) as Event;
  } catch {
    continue;
  }
  const who = event.parent_tool_use_id ? "  agent" : "orchestrator";
  if (event.type === "assistant") {
    for (const block of event.message?.content ?? []) {
      const said = describe(block);
      if (said) console.log(`${time()} ${who} ${said}`);
    }
  } else if (event.type === "result") {
    console.log(`${time()} session ${event.subtype ?? "ended"}: ${event.num_turns ?? "?"} turns${event.total_cost_usd !== undefined ? `, $${event.total_cost_usd.toFixed(2)}` : ""}`);
  }
}
