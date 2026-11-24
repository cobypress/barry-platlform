import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { AUTOMATIC_FORBIDDEN, readerText } from "../narrative/prompt.ts";
import { CONSULTANT_NEXT_STEPS } from "../render/exec-pages.ts";
import { MICROSITE_KEY_VAR } from "../microsite/seal.ts";

// An automatic run's three PDFs, made when the customer asks in the portal and
// kept sealed for it (DECISIONS.md 698). Nobody reads them before the
// customer does, so beside decision 682's number check and repair, the text
// Claude drafted and the page's fixed frame are read for any word that would
// claim a person, a review or a draft that isn't there; a hit keeps that
// document for us. The engine's own texts and the org's words are not
// searched: they hold such words honestly. Sealed as the
// microsite is (AES-256-GCM, a fresh 12-byte IV, a 16-byte tag), under a key
// of its own, with the run id and the output bound in, so a row moved to
// another run or output doesn't open.

export const DOCUMENTS_KEY_VAR = "HC_DOCUMENTS_KEY";
export const DOCUMENTS_SWITCH_VAR = "HC_DOCUMENTS_ON_DEMAND";

/** The three documents a customer may make, as the console's CHECK names them (migration 20261212). */
export const PORTAL_OUTPUTS = ["executive_summary", "full_health_check", "ceo_one_pager"] as const;
export type PortalOutput = (typeof PORTAL_OUTPUTS)[number];

/** Kept 7 days, the most the console's CHECK allows. */
export const PORTAL_DOCUMENT_DAYS = 7;

/** The sealed bytes' layout: version, IV, tag, then the ciphertext, which is as long as the PDF. */
export const SEAL_VERSION = 0x01;
const IV_BYTES = 12;
const TAG_BYTES = 16;
export const SEAL_OVERHEAD = 1 + IV_BYTES + TAG_BYTES;

/** The console's CHECK on "sealed": 1 byte to 15 MiB. */
export const SEALED_MAX_BYTES = 15 * 1024 * 1024;
/**
 * The largest PDF we seal: 4 MiB. The portal opens a sealed document whole
 * (the seal's tag must be checked before any byte leaves) and answers from a
 * serverless function whose response is capped at about 4.5 MB, so a larger
 * document could be made and never downloaded. The synthetic org's full
 * health check weighs well under 1 MiB, most of it embedded fonts; a big
 * org's adds pages, not fonts. A larger PDF is held for us.
 */
export const PORTAL_PDF_MAX_BYTES = 4 * 1024 * 1024;

/**
 * What a held-for-us row stores in "sealed", which can't be null or empty: one
 * zero byte. It isn't a sealed value (that starts with SEAL_VERSION and is at
 * least SEAL_OVERHEAD bytes long), opens under no key, and "PortalDocument"
 * never shows a held row anyway.
 */
export const HELD_PLACEHOLDER = Buffer.from([0x00]);

export class DocumentsKeyError extends Error {}

/**
 * HC_DOCUMENTS_KEY: exactly 32 bytes of base64, and different from the
 * microsite's and both connect keys, so no other key opens a customer's
 * documents. Null when unset. Errors never repeat a value.
 */
export function documentsKey(env: Record<string, string | undefined>): Buffer | null {
  const raw = env[DOCUMENTS_KEY_VAR]?.trim();
  if (!raw) return null;
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(raw)) throw new DocumentsKeyError(`${DOCUMENTS_KEY_VAR} isn't base64`);
  const key = Buffer.from(raw, "base64");
  if (key.length !== 32) throw new DocumentsKeyError(`${DOCUMENTS_KEY_VAR} must be 32 bytes, base64`);
  for (const other of ["HC_CONNECT_KEY", "HC_TOKEN_KEY", MICROSITE_KEY_VAR]) {
    const value = env[other]?.trim();
    if (value && Buffer.from(value, "base64").equals(key)) throw new DocumentsKeyError(`${DOCUMENTS_KEY_VAR} must differ from ${other}`);
  }
  return key;
}

/** The worker's switch and key, read once at start. A key that's missing or bad leaves the switch useless: every portal make is refused with `keyProblem`. */
export type PortalDocumentsSwitch = { on: boolean; key: Buffer | null; keyProblem: string | null };

/** The associated data: the run id and the output, which can't hold a colon (the run id is letters, digits, "_" and "-"). */
export const associatedData = (runId: string, output: PortalOutput): Buffer => Buffer.from(`${runId}:${output}`, "utf8");

/** `iv` is only for a fixed test vector. Everything else gets a fresh random IV. */
export function sealDocument(pdf: Buffer, runId: string, output: PortalOutput, key: Buffer, iv: Buffer = randomBytes(IV_BYTES)): Buffer {
  if (key.length !== 32) throw new DocumentsKeyError("The key must be 32 bytes");
  if (iv.length !== IV_BYTES) throw new Error("The IV must be 12 bytes");
  if (!runId || !PORTAL_OUTPUTS.includes(output)) throw new Error("The run id and output are needed as associated data");
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(associatedData(runId, output));
  const ciphertext = Buffer.concat([cipher.update(pdf), cipher.final()]);
  return Buffer.concat([Buffer.from([SEAL_VERSION]), iv, cipher.getAuthTag(), ciphertext]);
}

/** The console's side, here for the tests and as its reference: throws for a row of another run or output, another key, or tampered bytes. */
export function openDocument(sealed: Buffer, runId: string, output: PortalOutput, key: Buffer): Buffer {
  if (sealed.length < SEAL_OVERHEAD || sealed[0] !== SEAL_VERSION) throw new Error("Not a sealed document");
  const iv = sealed.subarray(1, 1 + IV_BYTES);
  const tag = sealed.subarray(1 + IV_BYTES, SEAL_OVERHEAD);
  const decipher = createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAAD(associatedData(runId, output));
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(sealed.subarray(SEAL_OVERHEAD)), decipher.final()]);
}

/**
 * Words Claude's own text on an automatic run may not hold where it prints:
 * everything the draft guard refuses (AUTOMATIC_FORBIDDEN), and "consultant",
 * "reviewed" and "Prepared by" on their own. Read only in the sections Claude
 * drafted (DECISIONS.md 698): the engine's own finding texts and the org's
 * words ("Forecast changed by hand", a profile called "Account Manager") are
 * never searched, since they would hold a real run's documents for nothing.
 */
export const DRAFT_FORBIDDEN: ReadonlyArray<{ label: string; pattern: RegExp }> = [
  ...AUTOMATIC_FORBIDDEN.map((pattern, i) => ({ label: `draft guard ${i + 1}`, pattern })),
  { label: "consultant", pattern: /\bconsultants?\b/i },
  { label: "reviewed", pattern: /\breviewed\b/i },
  { label: "prepared by", pattern: /\bprepared\s+by\b/i },
];

/** The labels of the forbidden words in a text Claude drafted, read as its reader sees it. Never the words around them. */
export function forbiddenInDraft(body: string): string[] {
  const read = readerText(body);
  return DRAFT_FORBIDDEN.filter((f) => f.pattern.test(body) || f.pattern.test(read)).map((f) => f.label);
}

const phrase = (text: string): RegExp => new RegExp(readerText(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/ /g, "\\s+"), "i");

/**
 * The page's fixed frame on an automatic run (DECISIONS.md 698): the cover's
 * and the one-pager's "Prepared by … Black Cloud" or "by …, Black Cloud" line,
 * the draft mark, and every promise of a person's time a consultant-led run's
 * next steps make. None of it is ever printed on an automatic run; a page
 * that shows one is held for us.
 */
export const FRAME_FORBIDDEN: ReadonlyArray<{ label: string; pattern: RegExp }> = [
  { label: "prepared by", pattern: /\b(?:prepared\s+)?by\s+(?:[^.,|]{1,120},\s+)?Black\s+Cloud\b/i },
  { label: "draft mark", pattern: /\bnot\s+approved\s+for\s+release\b/i },
  { label: "consultant's next steps", pattern: phrase(CONSULTANT_NEXT_STEPS.first.title) },
  { label: "consultant's next steps", pattern: phrase(CONSULTANT_NEXT_STEPS.first.text) },
  { label: "consultant's next steps", pattern: phrase(CONSULTANT_NEXT_STEPS.closing) },
  { label: "consultant's next steps", pattern: phrase(CONSULTANT_NEXT_STEPS.recheck) },
  { label: "consultant's next steps", pattern: phrase(CONSULTANT_NEXT_STEPS.recheckFull) },
];

/** A page's text as it prints, footers, chart labels and alt text included. */
export const pageText = readerText;

/** The labels of the frame's forbidden words a page's HTML holds. */
export function forbiddenInFrame(html: string): string[] {
  const text = readerText(html);
  return [...new Set(FRAME_FORBIDDEN.filter((f) => f.pattern.test(text)).map((f) => f.label))];
}
