import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { font } from "../design/tokens.ts";

// Embedded as data URLs: a render can't depend on the network, and a font that
// failed to load would silently change every line break.

const WEIGHTS = Object.values(font.weights);
let cached: string | null = null;

export function fontFaces(): string {
  if (cached) return cached;
  const require = createRequire(import.meta.url);
  const folder = join(dirname(require.resolve("@fontsource/poppins/package.json")), "files");
  cached = WEIGHTS.map((weight) => {
    const data = readFileSync(join(folder, `poppins-latin-${weight}-normal.woff2`)).toString("base64");
    return `@font-face { font-family: "${font.family}"; font-style: normal; font-weight: ${weight}; src: url(data:font/woff2;base64,${data}) format("woff2"); }`;
  }).join("\n");
  return cached;
}
