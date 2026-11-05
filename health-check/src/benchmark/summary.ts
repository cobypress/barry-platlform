import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { BENCHMARK_MIN_GROUP, BenchmarkRow } from "../schemas/index.ts";
import { BENCHMARK_FILE, BENCHMARK_WINDOW_QUARTERS, defaultBenchmarkDir, quarterIndex } from "./export.ts";

// Counts only. The summary says whether a comparison could be shown, and never
// prints a row or a value, so it's safe to paste anywhere.

export type BenchmarkGroup = {
  metricKey: string;
  industry: string;
  headcountBand: string;
  userBand: string;
  dealSizeBand: string;
  rows: number;
  enough: boolean;
};

export type BenchmarkSummary = { path: string; exists: boolean; rows: number; olderRows: number; invalidLines: number; groups: BenchmarkGroup[] };

/** Every row in a store file that matches the strict format, and how many lines didn't. */
export function readBenchmarkRows(path: string): { rows: BenchmarkRow[]; invalidLines: number } {
  const rows: BenchmarkRow[] = [];
  let invalidLines = 0;
  if (!existsSync(path)) return { rows, invalidLines };
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (!line.trim()) continue;
    let parsed;
    try {
      parsed = BenchmarkRow.safeParse(JSON.parse(line));
    } catch {
      parsed = null;
    }
    // A line that breaks the strict format isn't counted, so a bad write can't
    // push a group over the minimum.
    if (!parsed?.success) {
      invalidLines++;
      continue;
    }
    rows.push(parsed.data);
  }
  return { rows, invalidLines };
}

/**
 * Only the latest window counts. An org goes in once per window, so a repeat
 * check a year on replaces its old figures rather than adding a second org.
 */
export function latestWindow(rows: BenchmarkRow[]): BenchmarkRow[] {
  if (rows.length === 0) return [];
  const latest = Math.max(...rows.map((r) => quarterIndex(r.period)));
  return rows.filter((r) => latest - quarterIndex(r.period) < BENCHMARK_WINDOW_QUARTERS);
}

export function summariseBenchmarks(storeDir: string = defaultBenchmarkDir()): BenchmarkSummary {
  const path = join(resolve(storeDir), BENCHMARK_FILE);
  if (!existsSync(path)) return { path, exists: false, rows: 0, olderRows: 0, invalidLines: 0, groups: [] };
  const groups = new Map<string, BenchmarkGroup>();
  const { rows: valid, invalidLines } = readBenchmarkRows(path);
  const current = latestWindow(valid);
  const rows = current.length;
  const olderRows = valid.length - rows;
  for (const row of current) {
    // Periods inside the window are pooled: a comparison group is like orgs, whatever the quarter.
    const key = [row.metricKey, row.industry, row.headcountBand, row.userBand, row.dealSizeBand].join("|");
    const group = groups.get(key) ?? { metricKey: row.metricKey, industry: row.industry, headcountBand: row.headcountBand, userBand: row.userBand, dealSizeBand: row.dealSizeBand, rows: 0, enough: false };
    group.rows++;
    group.enough = group.rows >= BENCHMARK_MIN_GROUP;
    groups.set(key, group);
  }
  const order = (g: BenchmarkGroup): string[] => [g.metricKey, g.industry, g.headcountBand, g.userBand, g.dealSizeBand];
  const sorted = [...groups.values()].sort((a, b) => {
    const [x, y] = [order(a), order(b)];
    const i = x.findIndex((v, n) => v !== y[n]);
    return i === -1 ? 0 : x[i]! < y[i]! ? -1 : 1;
  });
  return { path, exists: true, rows, olderRows, invalidLines, groups: sorted };
}

/** The summary as plain lines for the command line. */
export function formatSummary(summary: BenchmarkSummary): string[] {
  if (!summary.exists) return [`No benchmark rows yet at ${summary.path}.`];
  const lines = [`Benchmark set at ${summary.path}: ${summary.rows} rows in the latest ${BENCHMARK_WINDOW_QUARTERS} quarters. A group needs ${BENCHMARK_MIN_GROUP} before a comparison is shown.`];
  if (summary.olderRows > 0) lines.push(`${summary.olderRows} older rows aren't counted.`);
  if (summary.invalidLines > 0) lines.push(`${summary.invalidLines} lines don't match the row format and weren't counted.`);
  const count = (n: number): string => `${n} ${n === 1 ? "row" : "rows"}`;
  let metric = "";
  for (const g of summary.groups) {
    if (g.metricKey !== metric) {
      metric = g.metricKey;
      lines.push(`${metric}: ${count(summary.groups.filter((x) => x.metricKey === metric).reduce((sum, x) => sum + x.rows, 0))}`);
    }
    lines.push(`  ${g.industry}, headcount ${g.headcountBand}, users ${g.userBand}, deal size ${g.dealSizeBand}: ${count(g.rows)}, ${g.enough ? "enough to compare" : `below ${BENCHMARK_MIN_GROUP}`}`);
  }
  return lines;
}
