import { closeSync, openSync, readFileSync, readSync, renameSync, rmSync, statSync, truncateSync, writeSync } from "node:fs";

// Checkpoints, written and read without ever holding one as a single string
// (DECISIONS.md 345). A big org's opportunities checkpoint was 242 million
// characters: the string and its bytes doubled the extraction's memory, and
// at about twice that it would pass V8's limit on a string's length.
//
// The file is still plain JSON, so anything that parses it whole still can.
// It's compact, except that an array of SPILL_AT items or more puts each
// item on a line of its own:
//
//   {"tasks":{"status":"ok","data":[
//   {"Id":"00T…","OwnerId":"005…"},
//   {"Id":"00T…","OwnerId":"005…"}
//   ],"source":{…}}}
//
// Compact JSON has no line breaks of its own, since a string's are escaped, so
// a line that ends with "[" opens such an array and a line that starts with
// "]" closes it. The reader rebuilds the rest around them. A checkpoint from
// before, written with two-space indents, is read whole, as it was.

/** Arrays this long or longer are written an item to a line. */
export const SPILL_AT = 1000;

const CHUNK = 4 * 1024 * 1024;
/** Stands in for a spilled array while the rest is parsed. Escaped, so no data key can match it. */
const SPILL_KEY = "\u0000spill";

const plain = (value: object): boolean => {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

/**
 * Writes `value` as JSON.stringify would, but in pieces, with long arrays an item
 * to a line. It's written beside the checkpoint and renamed over it once whole,
 * so a write that stops part-way leaves the last good one (DECISIONS.md 397).
 */
export function writeCheckpoint(path: string, value: unknown): void {
  const temp = `${path}.${process.pid}.writing`;
  try {
    writeWhole(temp, value);
    renameSync(temp, path);
  } catch (err) {
    rmSync(temp, { force: true });
    throw err;
  }
}

function writeWhole(path: string, value: unknown): void {
  const fd = openSync(path, "w", 0o600);
  let buffer = "";
  const put = (text: string): void => {
    buffer += text;
    if (buffer.length >= CHUNK) {
      writeSync(fd, buffer);
      buffer = "";
    }
  };
  // As JSON.stringify: undefined and functions leave a property out and become null in an array.
  const skipped = (v: unknown): boolean => v === undefined || typeof v === "function" || typeof v === "symbol";
  const emit = (v: unknown): void => {
    if (Array.isArray(v) && v.length >= SPILL_AT) {
      put("[\n");
      v.forEach((item, i) => put(`${skipped(item) ? "null" : JSON.stringify(item)}${i < v.length - 1 ? ",\n" : "\n"}`));
      put("]");
    } else if (v !== null && typeof v === "object" && !Array.isArray(v) && plain(v) && typeof (v as { toJSON?: unknown }).toJSON !== "function") {
      put("{");
      let first = true;
      for (const [k, child] of Object.entries(v)) {
        if (skipped(child)) continue;
        put(`${first ? "" : ","}${JSON.stringify(k)}:`);
        first = false;
        emit(child);
      }
      put("}");
    } else {
      put(JSON.stringify(v) ?? "null");
    }
  };
  try {
    emit(value);
    put("\n");
    writeSync(fd, buffer);
  } finally {
    closeSync(fd);
  }
}

/** Each line of a file, read a chunk at a time. Lines are cut at newline bytes, so no character is split between chunks. */
export function* fileLines(path: string): Generator<string> {
  const fd = openSync(path, "r");
  try {
    const bytes = Buffer.alloc(CHUNK);
    let rest = Buffer.alloc(0);
    for (;;) {
      const n = readSync(fd, bytes, 0, CHUNK, null);
      if (n === 0) break;
      const chunk = rest.length ? Buffer.concat([rest, bytes.subarray(0, n)]) : bytes.subarray(0, n);
      const cut = chunk.lastIndexOf(0x0a);
      if (cut === -1) {
        rest = Buffer.from(chunk);
        continue;
      }
      const lines = chunk.toString("utf8", 0, cut).split("\n");
      rest = Buffer.from(chunk.subarray(cut + 1));
      yield* lines;
    }
    if (rest.length) yield rest.toString("utf8");
  } finally {
    closeSync(fd);
  }
}

/** Reads a checkpoint written by writeCheckpoint, or any JSON file, without making it one string. */
export function readCheckpoint<T>(path: string): T {
  const lines = fileLines(path);
  const first = lines.next();
  if (first.done) throw new Error(`${path} is empty`);
  const second = lines.next();
  // Written before, with two-space indents: read whole, as it was.
  if (!second.done && /^\s*[[{]\s*$/.test(first.value) && second.value.startsWith("  ")) return JSON.parse(readFileSync(path, "utf8")) as T;

  const skeleton: string[] = [];
  const spills: unknown[][] = [];
  let open: unknown[] | null = null;
  const take = (line: string): void => {
    if (open) {
      if (line.startsWith("]")) {
        open = null;
        take(line.slice(1));
        return;
      }
      open.push(JSON.parse(line.endsWith(",") ? line.slice(0, -1) : line));
      return;
    }
    if (line.endsWith("[")) {
      open = [];
      skeleton.push(`${line.slice(0, -1)}${JSON.stringify({ [SPILL_KEY]: spills.length })}`);
      spills.push(open);
    } else skeleton.push(line);
  };
  take(first.value);
  if (!second.done) take(second.value);
  for (const line of lines) take(line);
  // A list still open at the end was cut short: a top-level list's skeleton would otherwise parse (DECISIONS.md 397).
  if ((open as unknown[] | null) !== null) throw new Error(`${path} was cut short: it ends inside a list, so it isn't complete`);
  return JSON.parse(skeleton.join(""), (_key, value) => (value && typeof value === "object" && !Array.isArray(value) && SPILL_KEY in value ? spills[(value as Record<string, number>)[SPILL_KEY]!] : value)) as T;
}

/**
 * Cuts a line-a-record file back to its last whole line. A crash in the middle of
 * an append leaves half a line, which every later read would trip on, and which
 * the next append would run on from (DECISIONS.md 397).
 */
export function trimToLastLine(path: string): void {
  const size = statSync(path).size;
  if (size === 0) return;
  const fd = openSync(path, "r");
  let keep = 0;
  try {
    const block = Buffer.alloc(64 * 1024);
    for (let end = size; end > 0; end -= block.length) {
      const start = Math.max(0, end - block.length);
      const n = readSync(fd, block, 0, end - start, start);
      const at = block.subarray(0, n).lastIndexOf(0x0a);
      if (at !== -1) {
        keep = start + at + 1;
        break;
      }
    }
  } finally {
    closeSync(fd);
  }
  if (keep !== size) truncateSync(path, keep);
}
