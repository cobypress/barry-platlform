/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
import type { Page } from "playwright";
import { buildDataset, checkNumbers, type FigureSet } from "../numbercheck/index.ts";

// Item 9, and the last gate before a PDF is written. Runs in the real browser
// against the real page boxes, so it sees what will actually print.

export type LayoutProblem = { page: number; check: string; detail: string };

export type LayoutReport = { pages: number; problems: LayoutProblem[]; passed: boolean };

export type LayoutExpectation = {
  minPages: number;
  maxPages: number;
  /** Rendered text must trace to this. Leave out only in a test of the layout alone. */
  dataset?: FigureSet;
};

type PageFacts = {
  page: number;
  width: number;
  height: number;
  overflowX: boolean;
  overflowY: boolean;
  outside: string[];
  cover: boolean;
  text: string;
  emptyCharts: string[];
  hasChart: boolean;
};

const MIN_TEXT = 40;
const RAW_DATE = /(?<![\w-])\d{4}-\d{2}-\d{2}(?![\w-])/;
const LEFTOVERS = /\[\[|\]\]|\bundefined\b|\bNaN\b|\[object Object\]|\bnull\b/;

async function pageFacts(browserPage: Page): Promise<PageFacts[]> {
  return browserPage.evaluate(() => {
    const TOLERANCE = 1.5;
    return [...document.querySelectorAll<HTMLElement>(".page")].map((el, i) => {
      const box = el.getBoundingClientRect();
      const outside: string[] = [];
      for (const child of el.querySelectorAll<HTMLElement>("*")) {
        if (child.closest("svg") && child.tagName !== "svg") continue;
        const rect = child.getBoundingClientRect();
        if (rect.width === 0 && rect.height === 0) continue;
        if (rect.bottom > box.bottom + TOLERANCE || rect.right > box.right + TOLERANCE || rect.left < box.left - TOLERANCE || rect.top < box.top - TOLERANCE) {
          outside.push(`${child.tagName.toLowerCase()}: ${(child.textContent ?? "").trim().slice(0, 60)}`);
        }
      }
      // Footers sit on the page but mustn't collide with what's above them.
      const footer = el.querySelector<HTMLElement>("[data-chrome]");
      if (footer) {
        const top = footer.getBoundingClientRect().top;
        for (const child of el.children) {
          if (child === footer) continue;
          if (child.getBoundingClientRect().bottom > top + TOLERANCE) outside.push(`runs into the footer: ${(child.textContent ?? "").trim().slice(0, 60)}`);
        }
      }
      // Node by node: textContent would run neighbouring cells into one figure.
      const parts: string[] = [];
      const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
      for (let node = walker.nextNode(); node; node = walker.nextNode()) {
        const parent = node.parentElement;
        if (!parent || parent.closest("[data-chrome], .tick")) continue;
        const value = (node.textContent ?? "").trim();
        if (value) parts.push(value);
      }
      const emptyCharts = [...el.querySelectorAll<HTMLElement>("[data-expect-chart]")]
        .filter((slot) => {
          const svg = slot.querySelector("svg");
          const gap = slot.querySelector(".chart-gap");
          if (gap && (gap.textContent ?? "").trim().length > 0) return false;
          if (!svg) return true;
          const rect = svg.getBoundingClientRect();
          return rect.width < 10 || rect.height < 10 || svg.querySelectorAll("path, rect, circle, line, text").length < 2;
        })
        .map((slot) => slot.dataset.expectChart ?? "chart");
      return {
        page: i + 1,
        width: box.width,
        height: box.height,
        overflowX: el.scrollWidth > el.clientWidth + TOLERANCE,
        overflowY: el.scrollHeight > el.clientHeight + TOLERANCE,
        outside,
        cover: el.classList.contains("cover"),
        text: parts.join(" | ").replace(/\s+/g, " "),
        hasChart: el.querySelector("[data-expect-chart]") !== null,
        emptyCharts,
      };
    });
  });
}

export async function checkLayout(browserPage: Page, expectation: LayoutExpectation): Promise<LayoutReport> {
  const facts = await pageFacts(browserPage);
  const problems: LayoutProblem[] = [];
  const push = (page: number, check: string, detail: string): void => void problems.push({ page, check, detail });

  if (facts.length < expectation.minPages || facts.length > expectation.maxPages) push(0, "page count", `${facts.length} pages, expected ${expectation.minPages} to ${expectation.maxPages}`);
  const fontsReady = await browserPage.evaluate(async () => {
    await document.fonts.ready;
    // check() answers true for a font that was never registered, so look at the faces.
    return [...document.fonts].some((face) => face.family.replace(/"/g, "") === "Poppins" && face.status === "loaded");
  });
  if (!fontsReady) push(0, "font", "Poppins didn't load");

  const first = facts[0];
  for (const f of facts) {
    if (f.overflowX || f.overflowY) push(f.page, "overflow", "content is larger than the page");
    for (const detail of f.outside.slice(0, 3)) push(f.page, "overflow", detail);
    if (first && (Math.abs(f.width - first.width) > 1 || Math.abs(f.height - first.height) > 1)) push(f.page, "page size", `${f.width} by ${f.height}`);
    for (const chart of f.emptyCharts) push(f.page, "missing chart", chart);
    if (!f.cover && !f.hasChart && f.text.length < MIN_TEXT) push(f.page, "empty section", `only "${f.text}"`);
    const leftover = LEFTOVERS.exec(f.text);
    if (leftover) push(f.page, "leftover", leftover[0]);
    const rawDate = RAW_DATE.exec(f.text);
    if (rawDate) push(f.page, "raw date", rawDate[0]);
    if (expectation.dataset) {
      for (const miss of checkNumbers(f.text, expectation.dataset).unmatched) push(f.page, "number check", `${miss.raw}: ${miss.reason}`);
    }
  }
  return { pages: facts.length, problems, passed: problems.length === 0 };
}

export const emptyDataset = (): FigureSet => buildDataset({ findings: [], metrics: [] });
