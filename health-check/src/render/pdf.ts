/// <reference lib="dom" />
import { chromium, type Browser } from "playwright";
import { checkLayout, type LayoutExpectation, type LayoutReport } from "./layout.ts";

// Chromium is only used to lay out and print HTML we built ourselves. Network
// access is cut off, so a report can't load anything or send anything anywhere.

export type PdfResult = { pdf: Buffer; layout: LayoutReport };

export const launchBrowser = (): Promise<Browser> => chromium.launch();

async function isolatedPage(browser: Browser) {
  const context = await browser.newContext({ javaScriptEnabled: true, offline: true });
  const page = await context.newPage();
  await page.route("**/*", (route) => (route.request().url().startsWith("data:") ? route.continue() : route.abort()));
  return { context, page };
}

/** Lays out the page, checks it, then prints it. The caller decides what a failed check means. */
export async function renderPdf(browser: Browser, html: string, expectation: LayoutExpectation): Promise<PdfResult> {
  const { context, page } = await isolatedPage(browser);
  try {
    await page.setContent(html, { waitUntil: "load" });
    await page.evaluate(() => document.fonts.ready.then(() => undefined));
    await page.emulateMedia({ media: "print" });
    const layout = await checkLayout(page, expectation);
    const pdf = await page.pdf({ preferCSSPageSize: true, printBackground: true });
    return { pdf, layout };
  } finally {
    await context.close();
  }
}

/** Layout only, for tests that feed in broken pages. */
export async function layoutOf(browser: Browser, html: string, expectation: LayoutExpectation): Promise<LayoutReport> {
  const { context, page } = await isolatedPage(browser);
  try {
    await page.setContent(html, { waitUntil: "load" });
    await page.evaluate(() => document.fonts.ready.then(() => undefined));
    await page.emulateMedia({ media: "print" });
    return await checkLayout(page, expectation);
  } finally {
    await context.close();
  }
}

/** Skia writes one /Type /Page object for each page. */
export function pdfPageCount(pdf: Buffer): number {
  return (pdf.toString("latin1").match(/\/Type\s*\/Page\b(?!s)/g) ?? []).length;
}
