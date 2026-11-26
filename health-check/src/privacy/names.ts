import { tokenCode, type PseudonymEntry, type Pseudonymiser } from "./pseudonymiser.ts";

// Rule 7, for free text. scrub() replaces whole names, usernames, emails and
// ids. People write "Sam said the forecast is a guess", so this replaces the
// parts of a name too: first names, surnames, runs such as "Mary Ann" or "van
// der Berg", the pieces of hyphenated and apostrophe names, and a few common
// diminutives. Each part gets the token its owner's full name gets, so a
// restored report reads "Sam Reed said...".
//
// Two functions share the vocabulary here but not the matching: maskNames()
// walks words and phrases and replaces them, and findNames() looks at every
// single run of letters on its own. findNames() runs on the finished prompt,
// so a gap in maskNames() refuses the send rather than leaking.

/** Kinds whose entries are people. Contacts and leads aren't labelled by any run yet, but would be people. */
export const PERSON_KINDS: ReadonlySet<string> = new Set(["user", "contact", "lead"]);

/** Ids of the entries that stand for a first name two or more people share. */
export const SHARED_NAME_PREFIX = "shared-name:";

// Words in shared-user names ("Shared Sales Desk", "Integration User"). They're
// never masked alone: "Sales Cloud" would become a token.
export const ORDINARY: ReadonlySet<string> = new Set(["sales", "shared", "desk", "integration", "user", "users", "admin", "system", "service", "support", "marketing", "automated", "process", "platform", "unknown", "team", "ops", "account", "lead", "data", "test", "api", "default", "workflow", "site", "guest", "the", "and", "chatter", "expert"]);

// Lower-case parts of a surname. Never masked alone, but kept inside a run.
const PARTICLES: ReadonlySet<string> = new Set(["van", "von", "de", "der", "den", "del", "della", "di", "da", "du", "la", "le", "los", "las", "dos", "das", "ter", "ten", "bin", "ibn", "st", "y", "e", "af", "av", "zu", "op"]);

// Words in company names that say nothing about which company it is.
const COMPANY: ReadonlySet<string> = new Set(["ltd", "limited", "plc", "llc", "llp", "inc", "incorporated", "corp", "corporation", "co", "company", "group", "holdings", "partners", "solutions", "systems", "services", "technologies", "technology", "international", "global", "uk", "europe", "gmbh", "sa", "bv", "ag", "pty"]);

// Mailbox names that aren't a person.
const MAILBOXES: ReadonlySet<string> = new Set(["info", "noreply", "no", "reply", "hello", "contact", "crm", "sfdc", "salesforce", "office", "enquiries", "accounts", "billing", "it", "help", "mail", "team"]);

// Name parts that are also ordinary English words, or business words. These are
// masked only when written with a capital ("Will", "WILL"), so "you will" and
// "a price book" survive but "Will said" doesn't. A capital at the start of a
// sentence is still masked: we prefer over-scrubbing to leaking.
export const COMMON_WORDS: ReadonlySet<string> = new Set([
  // Given names
  "will", "bill", "mark", "may", "sue", "rose", "pat", "rob", "bob", "rich", "art", "grant", "hope", "faith", "joy", "ray", "dawn", "summer", "june", "april", "august", "chase", "frank", "guy", "max", "jack", "nick", "sandy", "don", "gene", "ivy", "holly", "heather", "daisy", "iris", "jade", "ruby", "amber", "pearl", "crystal", "sky", "river", "cliff", "dale", "glen", "brook", "eve", "kit", "dot", "penny", "victor", "chance", "major", "earnest", "noble", "patience", "angel", "star", "sterling", "forest", "drew", "carol", "lance", "miles", "basil", "sunny", "storm", "wade", "hazel", "olive", "violet", "willow", "autumn", "honey", "candy", "ginger", "rusty", "buck", "duke", "earl", "king", "prince", "rex", "tiny",
  // Surnames
  "brown", "green", "white", "black", "grey", "gray", "young", "long", "price", "cash", "lane", "hunter", "rock", "stone", "wood", "woods", "hill", "hall", "ward", "cook", "baker", "mason", "turner", "fisher", "banks", "bell", "page", "sharp", "swift", "frost", "day", "good", "gold", "golden", "silver", "fox", "wolf", "lamb", "bird", "park", "best", "wise", "booth", "ford", "bush", "field", "fields", "marsh", "moss", "bond", "case", "rule", "cross", "little", "short", "small", "strong", "west", "east", "north", "south", "english", "french", "low", "keys", "key", "rice", "lake", "ash", "oak", "birch", "street", "mills", "knight", "bishop", "church", "house", "brooks", "rivers", "waters", "spring", "winter", "snow", "rain", "head", "pike", "hood", "lord", "law", "close", "worth", "gates", "gate", "wall", "walls", "pound", "bridge", "rush", "burns", "pace", "sale", "deal", "fair", "free", "glass", "steel", "iron", "tower", "power", "pool", "well", "wells", "judge", "porter", "man", "boss", "money", "budget", "bonus", "target", "quota", "miller", "potter", "archer", "farmer", "barber", "carpenter", "butler", "shepherd", "walker",
]);

// Common English diminutives, both ways. Cheap and partial: a nickname not here
// (or a pet name nobody would guess) gets through. See the log for why.
const DIMINUTIVES: Record<string, string[]> = {
  alexander: ["alex", "xander", "sandy"], alexandra: ["alex", "sandra", "sandy"], andrew: ["andy", "drew"], anthony: ["tony"], benjamin: ["ben", "benji"],
  catherine: ["cath", "kate", "katie", "kathy", "kat"], katherine: ["kate", "katie", "kathy", "kat"], kathryn: ["kate", "katie", "kathy"], charles: ["charlie", "chas"], charlotte: ["charlie", "lottie"],
  christopher: ["chris", "kit"], christine: ["chris", "tina"], christina: ["chris", "tina"], daniel: ["dan", "danny"], david: ["dave", "davy"], deborah: ["deb", "debbie"],
  edward: ["ed", "eddie", "ted", "ned"], elizabeth: ["liz", "lizzie", "beth", "betty", "eliza", "libby"], emily: ["em", "emmy"], frederick: ["fred", "freddie"], gregory: ["greg"],
  james: ["jim", "jimmy", "jamie"], jennifer: ["jen", "jenny"], jonathan: ["jon", "jonny"], joseph: ["joe", "joey"], joshua: ["josh"], kenneth: ["ken", "kenny"], lawrence: ["larry"],
  margaret: ["maggie", "meg", "peggy"], matthew: ["matt"], michael: ["mike", "mick", "mikey"], nicholas: ["nick", "nicky"], nathan: ["nate", "nat"], nathaniel: ["nate", "nat"],
  oliver: ["ollie"], patricia: ["pat", "patty", "trish"], patrick: ["pat", "paddy"], peter: ["pete"], philip: ["phil"], phillip: ["phil"], rebecca: ["becky", "becca"],
  richard: ["rich", "rick", "ricky", "richie"], robert: ["rob", "bob", "bobby", "robbie", "bert"], samuel: ["sam", "sammy"], samantha: ["sam", "sammy"], stephen: ["steve"], steven: ["steve"],
  susan: ["sue", "susie"], thomas: ["tom", "tommy"], timothy: ["tim"], victoria: ["vicky", "tori"], william: ["will", "bill", "billy", "liam"], zachary: ["zach", "zack"],
  abigail: ["abby"], jacqueline: ["jackie"], jessica: ["jess"], jacob: ["jake"], isabella: ["bella", "izzy"], isabel: ["izzy"], gabriel: ["gabe"], gabrielle: ["gabby"],
};

const FORMAL_OF = new Map<string, string[]>();
for (const [formal, nicknames] of Object.entries(DIMINUTIVES)) for (const n of nicknames) FORMAL_OF.set(n, [...(FORMAL_OF.get(n) ?? []), formal]);

// ---------------------------------------------------------------------------
// Folding. Matching runs on a folded copy of the text: accents and other marks
// dropped, compatibility forms such as full-width letters made plain, invisible
// format characters removed, and every apostrophe and hyphen made ASCII. Case is
// kept, because a capital decides whether a common word is a name. Each folded
// character remembers where it came from, so replacements land on the original.

const SPECIAL: Record<string, string> = { ø: "o", Ø: "O", ł: "l", Ł: "L", ß: "ss", ẞ: "SS", æ: "ae", Æ: "Ae", œ: "oe", Œ: "Oe", đ: "d", Đ: "D", ı: "i", þ: "th", Þ: "Th", ð: "d", Ð: "D", ħ: "h", Ħ: "H" };
const APOSTROPHE = /^['‘’ʼ`´′＇]$/u;
const HYPHEN = /^[‐‑‒–—−﹣－]$/u;

export type Folded = { text: string; start: number[]; end: number[] };

export function fold(text: string): Folded {
  let out = "";
  const start: number[] = [];
  const end: number[] = [];
  let i = 0;
  for (const ch of text) {
    const f = APOSTROPHE.test(ch) ? "'" : HYPHEN.test(ch) ? "-" : (SPECIAL[ch] ?? ch.normalize("NFKD").replace(/[\p{M}\p{Cf}]/gu, ""));
    for (let k = 0; k < f.length; k++) {
      start.push(i);
      end.push(i + ch.length);
    }
    out += f;
    i += ch.length;
  }
  return { text: out, start, end };
}

const foldWord = (word: string): string => fold(word).text;
/** The lookup key for a word: folded, lower case, apostrophes and hyphens dropped. */
export const keyOf = (word: string): string => foldWord(word).toLowerCase().replace(/['-]/g, "");
/** Masked only with a capital. */
export const isStrictWord = (key: string): boolean => key.length <= 2 || COMMON_WORDS.has(key);

const WORD = /\p{L}+(?:['-]\p{L}+)*/gu;
const NAME_WORD = /[\p{L}\p{M}]+(?:['’ʼ‐‑-][\p{L}\p{M}]+)*/gu;
const TOKEN = new RegExp(String.raw`\[\[[a-z]+(?::${tokenCode()})?\]\]`, "g");
const capitalised = (word: string): boolean => /^\p{Lu}/u.test(word);

// ---------------------------------------------------------------------------
// The index: every partial name, by key, with whose it is.

type Slot = {
  /** Whose part this is. A nickname or mailbox owner only counts when nobody has the part as written. */
  tokens: Set<string>;
  derived: Set<string>;
  display: string;
  /** Every owner is a company, not a person, so a capital is always needed. */
  strict: boolean;
};
type Variant = { text: string; derived: boolean };
export type NameIndex = {
  /** Single words and runs ("mary ann"), keyed by space-joined word keys. */
  phrases: Map<string, Slot>;
  /** Every single word, and every run of letters inside one, that findNames() looks for. */
  atoms: Map<string, Slot>;
  maxRun: number;
  /** Shared-name token by key, for words two or more owners have. */
  shared: Map<string, string>;
};

function singleAllowed(key: string, person: boolean): boolean {
  if (key.length < 2 || ORDINARY.has(key) || PARTICLES.has(key)) return false;
  return person || !COMPANY.has(key);
}

/** The partial names of one entry, as display forms. Runs are space-joined. */
function variantsOf(entry: PseudonymEntry, person: boolean): Variant[] {
  if (!person && entry.name === entry.id) return []; // An account labelled by its id: no name to mask.
  const words = entry.name.normalize("NFC").match(NAME_WORD) ?? [];
  const keys = words.map(keyOf);
  if (keys.every((k) => ORDINARY.has(k))) return [];
  const exact: string[] = [];
  const derived: string[] = [];
  for (let i = 0; i < words.length; i++) {
    for (let j = i; j < words.length; j++) {
      if (j === i) {
        if (singleAllowed(keys[i]!, person)) exact.push(words[i]!);
        // The pieces of "Mary-Jane" and "O'Brien".
        for (const piece of words[i]!.split(/['’ʼ‐‑-]/)) if (piece !== words[i] && singleAllowed(keyOf(piece), person)) exact.push(piece);
      } else if (!keys.slice(i, j + 1).every((k) => ORDINARY.has(k) || PARTICLES.has(k) || (!person && COMPANY.has(k)))) {
        exact.push(words.slice(i, j + 1).join(" "));
      }
    }
  }
  // "Mary Jane" and "Zoe Garcia" for "Mary-Jane" and "Zoë García-López".
  const pieces = words.flatMap((w) => w.split(/[\u2010\u2011-]/));
  if (pieces.length > words.length) {
    const pieceKeys = pieces.map(keyOf);
    for (let i = 0; i < pieces.length; i++) {
      for (let j = i + 1; j < pieces.length; j++) {
        if (!pieceKeys.slice(i, j + 1).every((k) => ORDINARY.has(k) || PARTICLES.has(k) || (!person && COMPANY.has(k)))) exact.push(pieces.slice(i, j + 1).join(" "));
      }
    }
  }
  if (person && keys[0]) {
    const related = [...(DIMINUTIVES[keys[0]] ?? []), ...(FORMAL_OF.get(keys[0]) ?? [])];
    for (const r of related) derived.push(r[0]!.toUpperCase() + r.slice(1));
  }
  if (person) {
    // A username's mailbox ("amorgan@") often isn't any part of the name.
    for (const alias of entry.aliases) {
      const local = alias.includes("@") ? alias.slice(0, alias.indexOf("@")) : null;
      for (const run of local?.match(/\p{L}+/gu) ?? []) {
        const k = keyOf(run);
        if (k.length >= 3 && singleAllowed(k, true) && !MAILBOXES.has(k)) derived.push(run);
      }
    }
  }
  return [...exact.map((text) => ({ text, derived: false })), ...derived.map((text) => ({ text, derived: true }))];
}

const cache = new WeakMap<Pseudonymiser, { size: number; index: NameIndex }>();

/**
 * Builds (or reuses) the index, and gives every shared first name its own
 * token. Call it once a run's people are all labelled, so the shared tokens are
 * saved in the pseudonym map; it's also called on every guard, in case.
 */
export function nameIndex(pseudonyms: Pseudonymiser): NameIndex {
  const hit = cache.get(pseudonyms);
  if (hit && hit.size === pseudonyms.entries().length) return hit.index;

  const phrases = new Map<string, Slot>();
  const atoms = new Map<string, Slot>();
  const add = (map: Map<string, Slot>, key: string, token: string, display: string, person: boolean, derived: boolean): void => {
    const slot = map.get(key) ?? { tokens: new Set<string>(), derived: new Set<string>(), display, strict: true };
    (derived ? slot.derived : slot.tokens).add(token);
    if (person) slot.strict = false;
    map.set(key, slot);
  };
  let maxRun = 1;
  for (const entry of pseudonyms.entries()) {
    if (entry.id.startsWith(SHARED_NAME_PREFIX)) continue;
    const person = PERSON_KINDS.has(entry.token.slice(2, entry.token.indexOf(":")));
    for (const { text, derived } of variantsOf(entry, person)) {
      const words = text.split(" ");
      maxRun = Math.max(maxRun, words.length);
      add(phrases, words.map(keyOf).join(" "), entry.token, text, person, derived);
      if (words.length === 1) {
        add(atoms, keyOf(text), entry.token, text, person, derived);
        for (const run of foldWord(text).match(/\p{L}+/gu) ?? []) if (singleAllowed(run.toLowerCase(), person)) add(atoms, run.toLowerCase(), entry.token, run, person, derived);
      }
    }
  }
  for (const slot of [...phrases.values(), ...atoms.values()]) if (slot.tokens.size === 0) slot.tokens = slot.derived;

  // Decision: a part two or more people share gets one token of its own, which
  // restores to the part as written ("Sam"). Giving it either person's token
  // would tell Claude, and then the reader, the wrong person; dropping it would
  // lose who the sentence is about. The bare first name is exactly what the
  // writer said, so the restored report loses nothing.
  const shared = new Map<string, string>();
  for (const [key, slot] of [...phrases, ...atoms].sort(([a], [b]) => a.localeCompare(b))) {
    if (slot.tokens.size < 2) continue;
    const token = shared.get(key) ?? pseudonyms.label("user", `${SHARED_NAME_PREFIX}${key}`, slot.display.replace(/^\p{Ll}/u, (c) => c.toUpperCase()));
    shared.set(key, token);
  }
  const index = { phrases, atoms, maxRun, shared };
  cache.set(pseudonyms, { size: pseudonyms.entries().length, index });
  return index;
}

/** Labels a token for every shared first name now, so it's saved with the run. */
export function registerSharedNames(pseudonyms: Pseudonymiser): void {
  nameIndex(pseudonyms);
}

const tokenFor = (index: NameIndex, key: string, slot: Slot): string => (slot.tokens.size === 1 ? [...slot.tokens][0]! : index.shared.get(key)!);

function spansOutside(text: string, pattern: RegExp): Array<[number, number]> {
  return [...text.matchAll(pattern)].map((m) => [m.index, m.index + m[0].length] as [number, number]);
}

// ---------------------------------------------------------------------------
// maskNames: words and phrases.

type Word = { text: string; start: number; end: number };

const caseOk = (words: string[], slot: Slot): boolean => words.every((w) => {
  const k = keyOf(w);
  if (PARTICLES.has(k)) return true;
  return !(slot.strict || isStrictWord(k)) || capitalised(w);
});

/** Replaces every partial name of every known person (and company) with its token. */
export function maskNames(text: string, pseudonyms: Pseudonymiser): string {
  return maskWords(maskShortIds(text, pseudonyms), pseudonyms);
}

// scrub() knows a user's 18-character id. Salesforce URLs and metadata often use
// the 15-character form, which is the same record.
function maskShortIds(text: string, pseudonyms: Pseudonymiser): string {
  let out = text;
  for (const entry of pseudonyms.entries()) {
    if (!PERSON_KINDS.has(entry.token.slice(2, entry.token.indexOf(":")))) continue;
    for (const alias of entry.aliases) {
      if (alias.length !== 18 || !RECORD_ID.test(alias) || !out.includes(alias.slice(0, 15))) continue;
      out = out.replace(new RegExp(`(?<![A-Za-z0-9])${alias.slice(0, 15)}(?:[A-Za-z0-9]{3})?(?![A-Za-z0-9])`, "g"), entry.token);
    }
  }
  return out;
}

function maskWords(text: string, pseudonyms: Pseudonymiser): string {
  const index = nameIndex(pseudonyms);
  if (index.phrases.size === 0) return text;
  const folded = fold(text);
  const tokens = spansOutside(folded.text, TOKEN);
  const inToken = (at: number): boolean => tokens.some(([s, e]) => at >= s && at < e);
  const words: Word[] = [...folded.text.matchAll(WORD)].filter((m) => !inToken(m.index)).map((m) => ({ text: m[0], start: m.index, end: m.index + m[0].length }));
  const hits: Array<[number, number, string]> = [];

  const lookup = (parts: string[]): [string, Slot] | null => {
    const key = parts.map(keyOf).join(" ");
    const slot = index.phrases.get(key);
    return slot && caseOk(parts, slot) ? [key, slot] : null;
  };

  for (let i = 0; i < words.length; i++) {
    // The longest run that can start here: words joined only by spaces or underscores.
    let last = i;
    while (last + 1 < words.length && last - i + 1 < index.maxRun && /^[\s_]+$/.test(folded.text.slice(words[last]!.end, words[last + 1]!.start))) last++;
    let matched = false;
    for (let j = last; j >= i && !matched; j--) {
      const run = words.slice(i, j + 1);
      const texts = run.map((w) => w.text);
      const possessive = /'s$/i.exec(texts[texts.length - 1]!);
      const tries: Array<[string[], number]> = [[texts, 0]];
      if (possessive) tries.push([[...texts.slice(0, -1), texts[texts.length - 1]!.slice(0, -2)], 2]);
      for (const [parts, trim] of tries) {
        const found = lookup(parts);
        if (!found) continue;
        hits.push([words[i]!.start, run[run.length - 1]!.end - trim, tokenFor(index, ...found)]);
        i = j;
        matched = true;
        break;
      }
    }
    if (matched) continue;
    // The pieces of a word with an apostrophe or hyphen. A common word inside a
    // hyphenated word is left: "Cross-sell" isn't about Ben Cross.
    const word = words[i]!;
    if (!/['-]/.test(word.text)) continue;
    for (const piece of word.text.matchAll(/\p{L}+/gu)) {
      const at = word.start + piece.index;
      const hyphenated = folded.text[at - 1] === "-" || folded.text[at + piece[0].length] === "-";
      const found = lookup([piece[0]]);
      if (!found) continue;
      if (hyphenated && (found[1].strict || isStrictWord(found[0]))) continue;
      hits.push([at, at + piece[0].length, tokenFor(index, ...found)]);
    }
  }
  if (hits.length === 0) return text;

  let out = "";
  let cursor = 0;
  for (const [s, e, token] of hits) {
    const from = folded.start[s]!;
    let to = folded.end[e - 1]!;
    // Swallow marks and invisible characters that trail the name in the original.
    while (to < text.length && /[\p{M}\p{Cf}]/u.test(text[to]!)) to++;
    out += text.slice(cursor, from) + token;
    cursor = to;
  }
  return out + text.slice(cursor);
}

// ---------------------------------------------------------------------------
// findNames: every run of letters on its own, as the last check before a send.

export type NameLeak = { token: string; what: "name" | "email" | "id" };
const EMAIL = /[\p{L}\p{N}._%+-]+@[\p{L}\p{N}-]+(?:\.[\p{L}\p{N}-]+)+/gu;
const RECORD_ID = /^[A-Za-z0-9]{15,18}$/;

/**
 * What's left of anyone's name, username, email or id in text that should hold
 * only tokens. Reports tokens, never the text it found, so the result can be
 * logged. Empty means clean.
 */
export function findNames(text: string, pseudonyms: Pseudonymiser): NameLeak[] {
  const index = nameIndex(pseudonyms);
  const leaks = new Map<string, NameLeak>();
  const folded = fold(text).text.replace(TOKEN, (t) => " ".repeat(t.length));
  if (EMAIL.test(folded)) leaks.set("email", { token: "[[email]]", what: "email" });
  EMAIL.lastIndex = 0;
  for (const m of folded.matchAll(/\p{L}+/gu)) {
    const key = m[0].toLowerCase();
    const slot = index.atoms.get(key);
    if (!slot) continue;
    const strict = slot.strict || isStrictWord(key);
    const at = m.index;
    const end = at + m[0].length;
    const hyphenated = (folded[at - 1] === "-" && /\p{L}/u.test(folded[at - 2] ?? "")) || (folded[end] === "-" && /\p{L}/u.test(folded[end + 1] ?? ""));
    if (strict && (!capitalised(m[0]) || hyphenated)) continue;
    const token = tokenFor(index, key, slot);
    leaks.set(token, { token, what: "name" });
  }
  for (const entry of pseudonyms.entries()) {
    if (!PERSON_KINDS.has(entry.token.slice(2, entry.token.indexOf(":")))) continue;
    for (const alias of entry.aliases) {
      if (RECORD_ID.test(alias) && text.includes(alias.slice(0, 15))) leaks.set(`${entry.token}:id`, { token: entry.token, what: "id" });
    }
  }
  return [...leaks.values()];
}
