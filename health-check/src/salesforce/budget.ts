import { BUDGET_DEFAULTS } from "../config.ts";

export type BudgetOptions = {
  /** Stop once the org's daily usage reaches this share of its limit. */
  stopAtShare: number;
  maxCallsPerRun?: number;
};

export type BudgetSnapshot = {
  stopAtShare: number;
  dailyLimit: number | null;
  usedAtStart: number | null;
  usedAtLastCheck: number | null;
  callsMade: number;
};

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
  #limit: number | null = null;
  #usedAtStart: number | null = null;
  #used: number | null = null;
  #calls = 0;

  constructor(options: BudgetOptions) {
    if (!(options.stopAtShare > 0 && options.stopAtShare <= 1)) throw new Error("stopAtShare must be above 0 and at most 1");
    this.stopAtShare = options.stopAtShare;
    this.maxCallsPerRun = options.maxCallsPerRun ?? BUDGET_DEFAULTS.maxCallsPerRun;
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

  /** Throws BudgetStopError if spending `cost` more calls would cross a limit. */
  assertCanSpend(cost = 1): void {
    if (this.#limit === null || this.#used === null) {
      throw new BudgetStopError("The org's API limit hasn't been read yet, so the run can't tell what's safe to spend");
    }
    if (this.#calls + cost > this.maxCallsPerRun) {
      throw new BudgetStopError(`This run reached its cap of ${this.maxCallsPerRun} API calls`);
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
