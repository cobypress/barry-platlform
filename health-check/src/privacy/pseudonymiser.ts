// Rule 7. Findings and metrics only ever hold tokens such as [[user:U0003]]. The
// map from token to name stays in the worker's run folder and is used in one
// place: when a report is rendered.
//
// scrub() is a backstop over everything the engine writes. It only knows whole
// names, so free text on its way to Claude (consultant chat in Milestone 5
// included) goes through guardPrompt() and sendPrompt() in src/narrative/guard.ts,
// which also mask first names, surnames and other parts (src/privacy/names.ts).

export type PseudonymEntry ={ token: string; id: string; name: string; aliases: string[] };

// A token's number is padded to four digits with no ceiling, so the 10,000th of a
// kind is [[user:U10000]]. Every pattern that finds a token is built from these
// two, so none stops at four digits (decision 464). A test holds that no other
// source file spells a token's digits itself.

/** A token's code as a regex source: its kind's letter (or `letter`, itself a source), then four digits or more. */
export const tokenCode = (letter = "[A-Z]"): string => String.raw`${letter}\d{4,}`;
/** A whole token as a regex source. `kinds` is an alternation such as "user" or "user|person|contact". */
export const tokenSource = (kinds = "[a-z]+", letter?: string): string => String.raw`\[\[(?:${kinds}):${tokenCode(letter)}\]\]`;

/**
 * The name the engine labels a user id with when it has none. It's a word for
 * "no name", not a name, so it isn't printed as one where a sentence says we
 * couldn't look them up (decision 474). Pseudonym maps already saved hold it,
 * so it never changes.
 */
export const UNKNOWN_USER = "Unknown user";

const TOKEN = new RegExp(tokenSource(), "g");
const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+/g;

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// The names Salesforce gives its own users. They're ordinary phrases ("give each
// integration user its own licence"), and nobody's name.
const SALESFORCE_USERS: ReadonlySet<string> = new Set(["integration user", "platform integration user", "automated process", "security user", "chatter expert", "insights integration"]);
export const isSalesforceUserName = (name: string): boolean => SALESFORCE_USERS.has(name.trim().toLowerCase().replace(/\s+/g, " "));

// A one-word name such as "Sales" can't be scrubbed from free text without
// mangling ordinary words ("Sales Cloud"), and nor can Salesforce's own users'
// names. Both are still tokenised wherever the engine labels a user directly.
// Other shared names, such as "Shared Sales Desk", are still scrubbed.
function isScrubbable(alias: string): boolean {
  if (isSalesforceUserName(alias)) return false;
  return alias.includes("@") || /\S\s+\S/.test(alias.trim()) || /^[A-Za-z0-9]{15,18}$/.test(alias);
}

export class Pseudonymiser {
  #byId = new Map<string, PseudonymEntry>();
  #counters = new Map<string, number>();
  #matcher: RegExp | null = null;
  /** Each alias, lower-cased, to its token. Built with the matcher, and dropped with it when a name is added. */
  #lookup: Map<string, string> | null = null;

  constructor(entries: PseudonymEntry[] = []) {
    for (const entry of entries) {
      this.#byId.set(entry.id, entry);
      const [kind, code] = entry.token.slice(2, -2).split(":");
      this.#counters.set(kind!, Math.max(this.#counters.get(kind!) ?? 0, Number(code!.slice(1))));
    }
  }

  /** The token for a record, created on first sight. `aliases` are other forms of the name, such as a username. */
  label(kind: string, id: string, name: string, aliases: string[] = []): string {
    const existing = this.#byId.get(id);
    if (existing) return existing.token;
    const n = (this.#counters.get(kind) ?? 0) + 1;
    this.#counters.set(kind, n);
    const token = `[[${kind}:${kind[0]!.toUpperCase()}${String(n).padStart(4, "0")}]]`;
    this.#byId.set(id, { token, id, name, aliases: [name, id, ...aliases].filter(Boolean) });
    this.#matcher = null;
    this.#lookup = null;
    return token;
  }

  has(id: string): boolean {
    return this.#byId.has(id);
  }

  #compile(): RegExp | null {
    if (this.#matcher) return this.#matcher;
    const aliases = [...this.#byId.values()].flatMap((e) => e.aliases).filter(isScrubbable);
    if (aliases.length === 0) return null;
    aliases.sort((a, b) => b.length - a.length);
    this.#matcher = new RegExp(`(?<![\\w@.])(?:${aliases.map(escapeRegex).join("|")})(?![\\w@])`, "gi");
    return this.#matcher;
  }

  /** Replaces every known name, username, email and id in free text, and any other email. */
  scrub(text: string): string {
    // Built once, not on every call: a big org scrubs hundreds of thousands of action list lines.
    if (!this.#lookup) {
      this.#lookup = new Map();
      for (const entry of this.#byId.values()) for (const alias of entry.aliases) this.#lookup.set(alias.toLowerCase(), entry.token);
    }
    const lookup = this.#lookup;
    const matcher = this.#compile();
    const named = matcher ? text.replace(matcher, (m) => lookup.get(m.toLowerCase()) ?? m) : text;
    return named.replace(EMAIL, "[[email]]");
  }

  /** scrub() over every string in a JSON-shaped value. Keys are left alone. */
  scrubDeep<T>(value: T): T {
    if (typeof value === "string") return this.scrub(value) as T;
    if (Array.isArray(value)) return value.map((v) => this.scrubDeep(v)) as T;
    if (value && typeof value === "object") {
      return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, this.scrubDeep(v)])) as T;
    }
    return value;
  }

  /** Report rendering only. An unknown token stays visible rather than vanishing. */
  restore(text: string): string {
    const names = new Map([...this.#byId.values()].map((e) => [e.token, e.name]));
    return text.replace(TOKEN, (token) => names.get(token) ?? token);
  }

  entries(): PseudonymEntry[] {
    return [...this.#byId.values()];
  }
}
