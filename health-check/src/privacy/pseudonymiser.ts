// Rule 7. Findings and metrics only ever hold tokens such as [[user:U0003]]. The
// map from token to name stays in the worker's run folder and is used in one
// place: when a report is rendered.
//
// scrub() is a backstop over everything the engine writes. It only knows whole
// names, so free text on its way to Claude (consultant chat in Milestone 5
// included) goes through guardPrompt() and sendPrompt() in src/narrative/guard.ts,
// which also mask first names, surnames and other parts (src/privacy/names.ts).

export type PseudonymEntry = { token: string; id: string; name: string; aliases: string[] };

const TOKEN = /\[\[([a-z]+):([A-Z]\d{4})\]\]/g;
const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+/g;

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// A one-word name such as "Sales" can't be scrubbed from free text without
// mangling ordinary words ("Sales Cloud"). Those are still tokenised wherever
// the engine labels a user directly.
function isScrubbable(alias: string): boolean {
  return alias.includes("@") || /\S\s+\S/.test(alias.trim()) || /^[A-Za-z0-9]{15,18}$/.test(alias);
}

export class Pseudonymiser {
  #byId = new Map<string, PseudonymEntry>();
  #counters = new Map<string, number>();
  #matcher: RegExp | null = null;

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
    const lookup = new Map<string, string>();
    for (const entry of this.#byId.values()) for (const alias of entry.aliases) lookup.set(alias.toLowerCase(), entry.token);
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
