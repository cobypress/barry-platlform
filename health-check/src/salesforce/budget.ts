import { BUDGET_DEFAULTS } from "../config.ts";

export type BudgetOptions = {
  /** Stop once the org's daily usage reaches this share of its limit. */
  stopAtShare: number;
  maxCallsPerRun?: number;
  /**
   * The most of the org's daily limit this run alone may spend (DECISIONS.md
   * 710): an automatic run's, so a small org's allowance is never mostly ours.
   */
  maxShareOfLimit?: number;
};

export type BudgetSnapshot = {
  stopAtShare: number;
  dailyLimit: number | null;
  usedAtStart: number | null;
  usedAtLastCheck: number | null;
  callsMade: number;
};

/**
 * Why a run stopped, as run.budget.stoppedReason opens: the console tells the
 * causes apart by these openings alone (its readers.ts budgetStopCause; this
 * repo's copy is CONSOLE_BUDGET_STOP_OPENINGS in src/worker/console-checks.ts,
 * DECISIONS.md 715), so each sentence below keeps its first words.
 */
export const DAILY_LIMIT_EXHAUSTED = "Salesforce reports the daily API limit is exhausted";

// Thrown before the call that would cross the line, so a run stops cleanly and
// can be resumed. Not a failure: the engine saves its place and exits.
export class BudgetStopError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BudgetStopError";
  }
}

export class ApiBudget {
  readonly stopAtShare: number;
  readonly maxCallsPerRun: number;
  readonly maxShareOfLimit: number | undefined;
  #limit: number | null = null;
  #usedAtStart: number | null = null;
  #used: number | null = null;
  #calls = 0;
  #limitUnread = false;

  constructor(options: BudgetOptions) {
    if (!(options.stopAtShare > 0 && options.stopAtShare <= 1)) throw new Error("stopAtShare must be above 0 and at most 1");
    this.stopAtShare = options.stopAtShare;
    this.maxCallsPerRun = options.maxCallsPerRun ?? BUDGET_DEFAULTS.maxCallsPerRun;
    this.maxShareOfLimit = options.maxShareOfLimit;
  }

  /** Fed by /limits and by each response's Sforce-Limit-Info header. */
  observe(used: number, limit: number): void {
    this.#limit = limit;
    this.#used = used;
    this.#usedAtStart ??= used;
  }

  observeHeader(header: string | null): void {
    const match = header?.match(/api-usage=(\d+)\/(\d+)/);
    if (match) this.observe(Number(match[1]), Number(match[2]));
  }

  /**
   * The org's daily limit couldn't be read (DECISIONS.md 702): until a
   * response's usage header gives the real figures, this connection may spend
   * only a small allowance, never an unlimited one.
   */
  limitUnread(): void {
    this.#limitUnread = true;
  }

  /** Throws BudgetStopError if spending `cost` more calls would cross a limit. */
  assertCanSpend(cost = 1): void {
    if (this.#limit === null || this.#used === null) {
      const allowance = Math.min(BUDGET_DEFAULTS.callsWithLimitUnread, this.maxCallsPerRun);
      if (this.#limitUnread && this.#calls + cost <= allowance) return;
      if (this.#limitUnread) throw new BudgetStopError(`The org's API limit couldn't be read, so this run stops at ${allowance} API calls`);
      throw new BudgetStopError("The org's API limit hasn't been read yet, so the run can't tell what's safe to spend");
    }
    if (this.#calls + cost > this.maxCallsPerRun) {
      throw new BudgetStopError(`This run reached its cap of ${this.maxCallsPerRun} API calls`);
    }
    if (this.maxShareOfLimit !== undefined && this.#calls + cost > Math.floor(this.#limit * this.maxShareOfLimit)) {
      throw new BudgetStopError(`This run reached its cap of ${Math.round(this.maxShareOfLimit * 100)}% of the org's ${this.#limit} daily API calls`);
    }
    const ceiling = Math.floor(this.#limit * this.stopAtShare);
    if (this.#used + cost > ceiling) {
      throw new BudgetStopError(
        `The org has used ${this.#used} of ${this.#limit} daily API calls, and this run stops at ${Math.round(this.stopAtShare * 100)}%`,
      );
    }
  }

  /** Counts a call. Usage moves on immediately in case the response has no header. */
  recordCall(cost = 1): void {
    this.#calls += cost;
    if (this.#used !== null) this.#used += cost;
  }

  get callsMade(): number {
    return this.#calls;
  }

  snapshot(): BudgetSnapshot {
    return {
      stopAtShare: this.stopAtShare,
      dailyLimit: this.#limit,
      usedAtStart: this.#usedAtStart,
      usedAtLastCheck: this.#used,
      callsMade: this.#calls,
    };
  }
}
