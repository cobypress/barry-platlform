// The characters a Word file can't hold, as one definition: wordSafe replaces
// them as it prints, and anything held to "plain text" may hold none of them,
// so the two can't drift. XML 1.0 has no place for most control characters, a
// lone half of a surrogate pair, U+FFFE or U+FFFF. Postgres's jsonb refuses
// U+0000 and a lone surrogate (SQLSTATE 22P05), which this covers too. Tab,
// line feed and carriage return are XML's own and are allowed here.

const UNSAFE_SOURCE = String.raw`[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]`;

/** Every character a Word file can't hold, for replacing. */
export const WORD_UNSAFE = new RegExp(UNSAFE_SOURCE, "g");

/** Whether text holds none of them: what would print, and store, exactly as it is. */
export function plainText(text: string): boolean {
  return !new RegExp(UNSAFE_SOURCE).test(text);
}
