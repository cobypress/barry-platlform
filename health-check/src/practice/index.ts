import { PRACTICE } from "./statements.ts";

// "What good looks like" (roadmap 3.1): for each area, a few things a customer
// can act on, each from a public Salesforce source or marked as Black Cloud's
// own practice. Data, not text the engine writes: no statement holds a figure,
// so none can contradict the number check.

export type PracticeSourceName = "Salesforce Well-Architected" | "Salesforce Architects" | "Trailhead";

export type PracticeSource = {
  name: PracticeSourceName;
  /** The page or unit's own title. */
  title: string;
  url: string;
  /** When we last read the page, YYYY-MM-DD. */
  checked: string;
  /** What the page says, in our words: never a quotation. */
  says: string;
};

/** One statement. With no source, it's Black Cloud's practice and says so wherever it's shown. */
export type Practice = { text: string; source: PracticeSource | null };

export { PRACTICE };

/** How many the chapter's "At a glance" slide shows. The rest are in the raw notes and on the microsite. */
export const PRACTICE_ON_SLIDE = 3;
export const OURS = "Black Cloud practice";

export const practiceFor = (domain: string): Practice[] => PRACTICE[domain] ?? [];

/** A statement's id among the raw notes' items. */
export const practiceId = (domain: string, index: number): string => `practice:${domain}:${index + 1}`;

/** Every statement's id for these areas, in order: the raw notes' items. */
export const practiceItems = (domains: string[]): string[] => domains.flatMap((d) => practiceFor(d).map((_, i) => practiceId(d, i)));

/** Where a statement comes from, in a few words. */
export const sourceLabel = (p: Practice): string => p.source?.name ?? OURS;
