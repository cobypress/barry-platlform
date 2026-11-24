// The Wilson score interval for a share (DECISIONS.md 706): unlike the plain
// normal interval it stays inside 0 to 1 and keeps its width near the ends, where
// stage probabilities often sit (10%, 90%), and with a few dozen deals.

export type Interval = { low: number; high: number };

/** The interval for `won` of `n` at `z` standard errors, as shares from 0 to 1. Null with no deals. */
export function wilson(won: number, n: number, z: number): Interval | null {
  if (n <= 0 || won < 0 || won > n) return null;
  const p = won / n;
  const z2 = z * z;
  const centre = (p + z2 / (2 * n)) / (1 + z2 / n);
  const half = (z / (1 + z2 / n)) * Math.sqrt((p * (1 - p)) / n + z2 / (4 * n * n));
  return { low: Math.max(0, centre - half), high: Math.min(1, centre + half) };
}

/** Whether a share (0 to 1) lies outside the interval. */
export const outside = (share: number, interval: Interval): boolean => share < interval.low || share > interval.high;
