// The engine's own vocabulary of letters-and-digits words (DECISIONS.md 682):
// Salesforce's setting and field names, our headcount bands and the research we
// cite, written in our source. Such a word is ours, not a claim about the org,
// so the number check accepts it wherever it's printed. Nothing else is accepted
// for looking harmless: a word that isn't here or in the run's data is held.
//
// Kept honest by src/numbercheck/__tests__/engine-words.test.ts, which reads every
// string in the folders that can reach a page (engine-words-scan.ts): a new word
// fails until review puts it here or in CODE_WORDS, and a word here that the
// source no longer writes fails too. Lower case, as identifiers are compared.

export const ENGINE_WORDS: ReadonlySet<string> = new Set(["25k-100k", "5k-25k", "ebsta-2025-deals-slipped", "ebsta-2025-new-business-sales-cycle", "ebsta-2025-new-logo-win-rate", "enableb2bmacampaigninfluence2", "enablecampaigninfluence2", "sbqq__opportunity2__c", "under-5k"]);

/**
 * Words the same folders write that never reach a page as text: CSS sizes, HTML
 * tags, colours, encodings and keys. Listed only so a new one is reviewed; the
 * number check never reads this list, so none of these is accepted on a page.
 */
export const CODE_WORDS: ReadonlySet<string> = new Set(["0.02em", "0.04em", "0.24in", "0.55in", "0.75em", "0.78em", "0.85em", "0.8em", "0.92em", "00g", "00z", "1.15fr", "1.25fr", "1.4em", "1.5fr", "10.5px", "1000px", "1040px", "10px", "11.5px", "11px", "12.5px", "12px", "13.5px", "13px", "14.5px", "14px", "15.5px", "15mm", "15px", "16.5px", "16px", "17mm", "17px", "18mm", "18px", "1_", "1e", "1fr", "1px", "2.6em", "20px", "22px", "24px", "26px", "28px", "2px", "3.2em", "3.4em", "300px", "30mm", "30px", "32px", "34px", "36px", "38px", "3px", "40px", "44px", "4px", "56px", "5px", "68px", "6px", "7px", "8mm", "8px", "9mm", "9px", "a1", "a4", "adoption.56", "base64", "base64url", "checks-3-2", "documented.59", "ff0563c1", "ff595959", "gray125", "h1", "h2", "h3", "h3.cap", "h3.sub", "h4", "h5", "latin1", "p1", "p2", "pay.49", "plan_90_day", "r4", "r4.3", "real-org.36", "rid1", "sha256", "t00", "utf-8", "utf8", "v1", "woff2", "x1", "x2", "y1", "y2"]);
