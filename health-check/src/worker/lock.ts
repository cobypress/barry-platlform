import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// One job at a time per run or link. The console gives every enqueue its own
// job id, so BullMQ will happily hand two jobs for the same run to two workers
// at once. Two engines on one run folder would interleave checkpoints, and two
// exchanges of one code would race to settle the link, so the second waits.
//
// The lock is a file made with O_EXCL, holding the owner's pid. A lock whose
// process is gone (a crash) is taken over. This assumes one worker host, which
// is also what the token store's files assume.

export class JobBusyError extends Error {
  constructor(key: string) {
    super(`Another job is already working on ${key}. This one will be retried`);
    this.name = "JobBusyError";
  }
}

const KEY = /^[A-Za-z0-9][A-Za-z0-9_-]{0,120}$/;

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM: it exists, but belongs to someone else.
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

export async function withJobLock<T>(dir: string, key: string, work: () => Promise<T>, options: { pid?: number; isAlive?: (pid: number) => boolean } = {}): Promise<T> {
  if (!KEY.test(key)) throw new Error("A lock key is letters, digits, hyphens and underscores only");
  const pid = options.pid ?? process.pid;
  const isAlive = options.isAlive ?? alive;
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const path = join(dir, `${key}.lock`);
  const take = (): boolean => {
    try {
      writeFileSync(path, JSON.stringify({ pid, at: new Date().toISOString() }), { flag: "wx", mode: 0o600 });
      return true;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
      return false;
    }
  };
  if (!take()) {
    let holder: number | undefined;
    try {
      holder = (JSON.parse(readFileSync(path, "utf8")) as { pid?: number }).pid;
    } catch {
      // Unreadable or half-written: treat as held, and let the retry look again.
    }
    if (holder === undefined || holder === pid || isAlive(holder)) throw new JobBusyError(key);
    rmSync(path, { force: true });
    if (!take()) throw new JobBusyError(key);
  }
  try {
    return await work();
  } finally {
    rmSync(path, { force: true });
  }
}
