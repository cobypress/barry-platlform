import { strFromU8, unzipSync } from "fflate";

// A Word file's words in reading order, for tests and for a person reviewing a
// sample without Word: each paragraph on its own, a heading marked with one #
// a level, a line break kept as one.

const decode = (value: string): string => value.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&apos;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&");

/** The document's own XML, as Word reads it. */
export function documentXml(bytes: Buffer): string {
  const xml = unzipSync(new Uint8Array(bytes))["word/document.xml"];
  if (!xml) throw new Error("No word/document.xml: this isn't a Word file");
  return strFromU8(xml);
}

export type DocxParagraph = { heading: number | null; text: string };

export function docxParagraphs(bytes: Buffer): DocxParagraph[] {
  const xml = documentXml(bytes);
  const body = xml.slice(xml.indexOf("<w:body>"));
  return [...body.matchAll(/<w:p(?:\s[^>]*)?>([\s\S]*?)<\/w:p>|<w:p(?:\s[^>]*)?\/>/g)].map((p) => {
    const inner = p[1] ?? "";
    const level = /<w:pStyle w:val="Heading(\d)"\/>/.exec(inner)?.[1];
    const text = [...inner.matchAll(/<w:t(?:\s[^>]*)?>([^<]*)<\/w:t>|<w:br\/>|<w:tab\/>/g)].map((m) => (m[0] === "<w:br/>" ? "\n" : m[0] === "<w:tab/>" ? "\t" : decode(m[1] ?? ""))).join("");
    return { heading: level ? Number(level) : null, text };
  });
}

/** The words as plain text, headings marked. */
export function docxText(bytes: Buffer): string {
  return docxParagraphs(bytes)
    .filter((p) => p.text.trim() !== "")
    .map((p) => (p.heading ? `${"#".repeat(p.heading)} ${p.text}` : p.text))
    .join("\n\n");
}
