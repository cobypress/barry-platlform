import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// One job at a time per run or link. The console gives every enqueue its own
// job id, so BullMQ will happily hand two jobs for the same run to two workers
// at once. Two engines on one run folder would interleave checkpoints, and two
// exchanges of one code would race to settle the link, so the second waits.
//
// The lock is a file made with O_EXCL, holding the owner's pid and a random
// token for this process. The token, not the pid, says a lock is ours: after a
// container restart the new process often has the same pid (1) as the one that
// died, and its locks must read as stale, not as held. A lock whose process is
// gone is taken over, but only while holding a second file, the breaker, so
// two processes that both find it stale can't both take it. This assumes one
// worker host, which is also what the token store's files assume.

export class JobBusyError extends Error {
  constructor(key: string) {
    super(`Another job is already working on ${key}. This one will be retried`);
    this.name = "JobBusyError";
  }
}

const KEY = /^[A-Za-z0-9][A-Za-z0-9_-]{0,120}$/;
const PROCESS_TOKEN = randomUUID();
/** Breaking a lock takes milliseconds, so a breaker this old was left by a crash. */
const BREAKER_STALE_MS = 60_000;

type Holder = { pid?: number; token?: string };

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM: it exists, but belongs to someone else.
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

export type LockOptions = {
  /** Tests only: stand in for another process. */
  pid?: number;
  token?: string;
  isAlive?: (pid: number) => boolean;
  /** Tests only: called after a lock is found stale and before it's taken over. */
  onStale?: () => void;
};

export async function withJobLock<T>(dir: string, key: string, work: () => Promise<T>, options: LockOptions = {}): Promise<T> {
  if (!KEY.test(key)) throw new Error("A lock key is letters, digits, hyphens and underscores only");
  const pid = options.pid ?? process.pid;
  const token = options.token ?? PROCESS_TOKEN;
  const isAlive = options.isAlive ?? alive;
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const path = join(dir, `${key}.lock`);
  const breaker = join(dir, `${key}.lock.breaker`);
  const mine = JSON.stringify({ pid, token, at: new Date().toISOString() });

  const create = (file: string): boolean => {
    try {
      writeFileSync(file, mine, { flag: "wx", mode: 0o600 });
      return true;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
      return false;
    }
  };
  const read = (file: string): Holder | null => {
    try {
      return JSON.parse(readFileSync(file, "utf8")) as Holder;
    } catch {
      // Unreadable or half-written: treat as held, and let the retry look again.
      return null;
    }
  };
  // Held by this process, or by another one that's still running. A lock of
  // this pid with someone else's token is an earlier process's, and is stale.
  const stale = (holder: Holder | null): boolean =>
    holder !== null && typeof holder.pid === "number" && holder.token !== token && (holder.pid === pid || !isAlive(holder.pid));

  if (!create(path)) {
    const seen = read(path);
    if (!stale(seen)) throw new JobBusyError(key);
    options.onStale?.();
    try {
      if (statSync(breaker).mtimeMs < Date.now() - BREAKER_STALE_MS) rmSync(breaker, { force: true });
    } catch {
      // No breaker: the usual case.
    }
    if (!create(breaker)) throw new JobBusyError(key);
    try {
      // Only the stale lock we saw is removed. If someone took it over meanwhile, it's theirs.
      const now = read(path);
      if (now?.token !== seen!.token || now?.pid !== seen!.pid || !stale(now)) throw new JobBusyError(key);
      rmSync(path, { force: true });
      if (!create(path)) throw new JobBusyError(key);
    } finally {
      rmSync(breaker, { force: true });
    }
  }
  try {
    return await work();
  } finally {
    // Only our own lock is removed, never one another process took over from us.
    if (read(path)?.token === token) rmSync(path, { force: true });
  }
}
