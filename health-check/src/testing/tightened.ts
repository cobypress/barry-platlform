import { LOOSENED, TIGHTENED, type Loosening, type Tightening } from "../engine/loosened.ts";

// Tests only (DECISIONS.md 754): no rule has been tightened, so the tests declare
// one for as long as a callback runs and take it away after, however it ends. A
// real declaration is written in src/engine/loosened.ts, never through this.

type Tables = { tightened?: Record<string, readonly Tightening[]>; loosened?: Record<string, readonly Loosening[]> };

export async function withDeclared<T>(tables: Tables, fn: () => T | Promise<T>): Promise<T> {
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
  }
}
