import { unreadReason, type MetadataBundle } from "../extract/metadata-parse.ts";
import type { Sourced } from "../extract/types.ts";
import { Handbook, HANDBOOK_VERSION } from "./schema.ts";

/**
 * In the run folder, beside the findings: kept by `hc purge`, deleted with the
 * folder, and read by nothing else in the engine or the worker (DECISIONS.md 452).
 */
export const HANDBOOK_FILE = "handbook.json";

const unavailable = (reason: string) => ({ status: "unavailable" as const, reason });

function bothUnavailable(reason: string): Handbook {
  return { version: HANDBOOK_VERSION, flows: unavailable(reason), validationRules: unavailable(reason) };
}

/**
 * The handbook's data from the metadata step's output. Undefined when the run
 * had no metadata step. A part that couldn't be read says why, so the documents
 * can tell "no flows" from "we couldn't read them" (decision 454). Checked
 * against the schema here, so a file that doesn't fit is never written: its
 * parts say so instead, and the run carries on.
 */
export function buildHandbook(metadata: Sourced<MetadataBundle> | undefined): Handbook {
  if (!metadata) return bothUnavailable("This run didn't read the org's metadata.");
  if (metadata.status !== "ok") return bothUnavailable(`Metadata couldn't be retrieved: ${metadata.reason}`);
  const detail = metadata.data.handbook;
  if (!detail) return bothUnavailable("This run's metadata was read before the handbook's detail was kept.");
  const unread = unreadReason(metadata.data, ["validationRules"]);
  const handbook = Handbook.safeParse({
    version: HANDBOOK_VERSION,
    flows: { status: "ok", items: detail.flows },
    validationRules: unread ? unavailable(unread) : { status: "ok", items: detail.validationRules },
  });
  if (handbook.success) return handbook.data;
  const where = handbook.error.issues[0]?.path.join(".") ?? "";
  return bothUnavailable(`The handbook's data didn't fit its format${where ? ` at ${where}` : ""}, so it wasn't kept.`);
}
