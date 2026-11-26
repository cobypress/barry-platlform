import { LOOSENED, TIGHTENED, type Loosening, type Tightening } from "../engine/loosened.ts";

// Tests only (DECISIONS.md 754): the one rule tightened (cur.stuck-in-stage 3,
// 777) runs only behind a switch, so the tests declare their own for as long as a callback runs and take it away after, however it ends. A
// real declaration is written in src/engine/loosened.ts, never through this.

type Tables = { tightened?: Record<string, readonly Tightening[]>; loosened?: Record<string, readonly Loosening[]> };

let open = 0;

/**
 * One at a time: each call restores the tables as it found them, so two that
 * overlap (nested, or awaited side by side) would put back each other's entries
 * or wipe them. A second call while one is open is refused.
 */
export async function withDeclared<T>(tables: Tables, fn: () => T | Promise<T>): Promise<T> {
  if (open > 0) throw new Error("withDeclared is already open: declarations for tests can't overlap");
  open++;
  const held: Array<[Record<string, unknown>, Record<string, unknown>]> = [];
  for (const [table, add] of [
    [TIGHTENED, tables.tightened],
    [LOOSENED, tables.loosened],
  ] as const) {
    if (!add) continue;
    const live = table as Record<string, unknown>;
    held.push([live, { ...live }]);
    Object.assign(live, add);
  }
  try {
    return await fn();
  } finally {
    for (const [live, before] of held) {
      for (const key of Object.keys(live)) delete live[key];
      Object.assign(live, before);
    }
    open--;
  }
}
