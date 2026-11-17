/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
import type { Page } from "playwright";

// No page may run past its footer, whatever the text (Coby, 7 November). The
// page builders size what they can, but Claude's wording on a real org can be
// far longer than the samples, so the last word is the browser's: before the
// layout check, anything that doesn't fit moves to a "continued" page straight
// after, at the first block that crosses the footer. A paragraph breaks between
// sentences rather than leaving a gap. Then every footer and the contents page
// are numbered again.
//
// What can't be split (a chart, a card, a row of figures) moves whole. One that
// is taller than a page on its own stays where it is, and the layout check
// reports it.

/** Moves what doesn't fit onto continued pages and renumbers. Returns how many pages it added. */
export async function carryOver(browserPage: Page): Promise<number> {
  return browserPage.evaluate(() => {
    const TOLERANCE = 1.5;
    const MAX_ADDED = 200;
    // Moved as a whole: a chart, a figure, a card, a table row, a row of figures.
    const ATOMIC = "svg, figure, article, tr, img, [data-expect-chart], [data-keep], .stat-row, .stat, .chart-gap";
    const SPLITTABLE_TAGS = new Set(["DIV", "SECTION", "OL", "UL", "TABLE", "TBODY"]);
    const TEXT_TAGS = new Set(["P", "LI"]);
    const HEADINGS = new Set(["H1", "H2", "H3", "H4", "H5"]);
    // A part's own heading, which a split part carries on with. A page's title and eyebrow are the continued page's.
    const LABELS = new Set(["H3", "H4", "H5"]);
    // What a part's heading heads and can be split in two: a list, a table, a paragraph.
    const CARRIES_ON = new Set(["UL", "OL", "TABLE", "TBODY", "P"]);

    const isChrome = (el: Element): boolean => el.hasAttribute("data-chrome");
    const kidsOf = (el: Element): HTMLElement[] => [...el.children].filter((c): c is HTMLElement => c instanceof HTMLElement && !isChrome(c) && c.tagName !== "STYLE" && c.tagName !== "THEAD");
    const box = (el: Element): DOMRect => el.getBoundingClientRect();
    const visible = (el: Element): boolean => {
      const r = box(el);
      return r.width > 0 || r.height > 0;
    };
    /**
     * Takes no room: no height, no words, and nothing inside with any height, as an empty
     * block or the shell an emptied column leaves. It neither stays nor goes (DECISIONS.md 576).
     */
    const roomless = (el: Element): boolean => box(el).height === 0 && !(el.textContent ?? "").trim() && [...el.querySelectorAll("*")].every((d) => box(d).height === 0);
    /** The lowest a page's content may reach: the footer's top, or the page's bottom less its padding. */
    const limitOf = (page: HTMLElement): number => {
      const footer = page.querySelector<HTMLElement>(":scope > [data-chrome]");
      const inner = box(page).bottom - parseFloat(getComputedStyle(page).paddingBottom || "0");
      return Math.min(footer ? box(footer).top : Infinity, inner) + TOLERANCE;
    };
    /** The bottom of an element's own content, including descendants that run past it. */
    const bottomOf = (el: Element): number => {
      // Its bottom margin counts: the page's scroll height includes it.
      let bottom = box(el).bottom + (parseFloat(getComputedStyle(el).marginBottom) || 0);
      for (const d of el.querySelectorAll("*")) {
        if (d.closest("svg") && d.tagName !== "svg") continue;
        if (visible(d)) bottom = Math.max(bottom, box(d).bottom);
      }
      return bottom;
    };
    const overflows = (page: HTMLElement): boolean =>
      page.scrollHeight > page.clientHeight + TOLERANCE || kidsOf(page).some((k) => visible(k) && (bottomOf(k) > limitOf(page) || box(k).top < box(page).top - TOLERANCE));
    /** Words that sit directly in a block, beside its child blocks: kidsOf doesn't see them. */
    const ownWords = (el: Element): boolean => [...el.childNodes].some((n) => n.nodeType === Node.TEXT_NODE && !!(n.textContent ?? "").trim());
    // A block with words of its own beside its children stays or goes whole: looked into, its
    // words were left behind or removed with it when its children went (DECISIONS.md 577).
    const splittable = (el: HTMLElement): boolean => SPLITTABLE_TAGS.has(el.tagName) && !el.matches(ATOMIC) && kidsOf(el).length > 0 && !ownWords(el);
    /** Children laid out side by side, as in a grid of cards, rather than one under another. */
    const sideBySide = (kids: HTMLElement[]): boolean => kids.some((k, i) => i > 0 && box(k).top < box(kids[i - 1]!).bottom - 1 && box(k).left > box(kids[i - 1]!).left + 1);
    /** Set in CSS columns: one list folded, read down one column and then the next, unlike a grid's blocks side by side. */
    const inColumns = (el: HTMLElement): boolean => (parseInt(getComputedStyle(el).columnCount, 10) || 1) > 1;
    const isHeading = (el: Element): boolean => HEADINGS.has(el.tagName) || el.classList.contains("eyebrow");
    const isLabel = (el: Element): boolean => LABELS.has(el.tagName) && !el.classList.contains("eyebrow");

    /** The shells made below, so one left holding only shells can be told from one holding what moved. */
    const made = new WeakSet<Node>();
    /** A copy without its children: the wrapper the moved content goes into. A table keeps its head. */
    const shell = (el: HTMLElement): HTMLElement => {
      const copy = el.cloneNode(false) as HTMLElement;
      copy.removeAttribute("data-page");
      if (el.tagName === "TABLE") {
        const head = el.querySelector(":scope > thead");
        if (head) made.add(copy.appendChild(head.cloneNode(true)));
      }
      made.add(copy);
      return copy;
    };
    /** Holds nothing but shells, at any depth: nothing that was on the page has gone into it. */
    const onlyShells = (el: Element): boolean => [...el.childNodes].every((n) => made.has(n) && (n.nodeName === "THEAD" || onlyShells(n as Element)));
    /** Holds anything printed: words, or a chart, a picture or a table row. A table's copied head is ours. */
    const printed = (el: Element): boolean =>
      [...el.childNodes].some((n) => (n instanceof Element ? n.tagName !== "THEAD" && n.tagName !== "STYLE" && (n.matches(ATOMIC) || printed(n)) : !!(n.textContent ?? "").trim()));
    /**
     * Takes no room: not shown, roomless, or a copy we made that nothing went into, whatever
     * height its padding or border gives it. It neither stays nor goes (DECISIONS.md 576, 577).
     * A copy is filled only while the block it copies is looked into, and what is moved goes
     * to the next slide, never into the slide being read: a copy judged empty here stays so.
     */
    const noRoom = (el: Element): boolean => !visible(el) || roomless(el) || (made.has(el) && onlyShells(el));
    /** Prints something of its own: words (a copied table head), a background, a border, an outline, a shadow, a ::before or ::after. */
    const marks = (el: Element): boolean => {
      const s = getComputedStyle(el);
      // A line with no style has a width all the same, so both are read.
      const edges = ["border-top", "border-right", "border-bottom", "border-left", "outline"].some((p) => s.getPropertyValue(`${p}-style`) !== "none" && parseFloat(s.getPropertyValue(`${p}-width`)) > 0);
      const pseudo = ["::before", "::after"].some((p) => !["none", "normal"].includes(getComputedStyle(el, p).content));
      return !!(el.textContent ?? "").trim() || edges || pseudo || s.backgroundImage !== "none" || s.boxShadow !== "none" || !/^(transparent|rgba\(0, 0, 0, 0\))$/.test(s.backgroundColor);
    };

    /**
     * Keeps the sentences of a paragraph that fit and moves the rest into a new
     * one. Null when not even the first sentence fits.
     */
    const splitText = (el: HTMLElement, limit: number): HTMLElement | null => {
      const ends: Array<{ node: Text; offset: number }> = [];
      const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
      for (let node = walker.nextNode() as Text | null; node; node = walker.nextNode() as Text | null) {
        // A full stop inside a figure ("2.5") isn't a sentence end.
        for (const m of (node.textContent ?? "").matchAll(/[.!?:;](?=\s+\S)/g)) ends.push({ node, offset: m.index + 1 });
      }
      for (let i = ends.length - 1; i >= 0; i--) {
        const range = document.createRange();
        range.setStart(ends[i]!.node, ends[i]!.offset);
        range.setEndAfter(el.lastChild!);
        const rest = range.extractContents();
        if (bottomOf(el) <= limit) {
          const tail = el.cloneNode(false) as HTMLElement;
          // The rest of a list item is the item going on, not a new one: it prints with no
          // marker (DECISIONS.md 571). A copy of a rest is still a rest.
          if (el.tagName === "LI") tail.setAttribute("data-carried-rest", "");
          tail.appendChild(rest);
          // The space after the sentence end opens the new paragraph.
          if (tail.firstChild?.nodeType === Node.TEXT_NODE) tail.firstChild.textContent = (tail.firstChild.textContent ?? "").replace(/^\s+/, "");
          return tail;
        }
        el.appendChild(rest);
        el.normalize();
      }
      return null;
    };

    type State = { placed: boolean; moved: boolean };

    /**
     * Moves everything from the first block that crosses the limit onward into
     * dest, keeping the wrappers it sits in. `placed` is whether some content
     * stays on the page, so a page is never left with only its heading.
     */
    const breakAt = (container: HTMLElement, dest: HTMLElement, limit: number, state: State): void => {
      const kids = kidsOf(container);
      /**
       * Where what goes from kid i starts. A part's heading doesn't stay at the foot
       * of a page without what it heads, so the part's headings straight above it go
       * too, as long as something else stays on the page (DECISIONS.md 508). Only a
       * part's own (H3 to H5): the page's eyebrow and title keep their rule (510).
       */
      const headedFrom = (i: number): number => {
        let from = i;
        while (from > 0 && state.placed && isLabel(kids[from - 1]!)) from--;
        return from;
      };
      /**
       * Kid `at` was split in two and its rest opens what goes on. If it is a list,
       * a table or a paragraph and the first thing under its heading, the rest
       * starts with a copy of that heading marked as continued, as the page's title
       * is. Anything else goes on unlabelled: a wrapper (whose rest can start with a
       * heading of its own, or be an empty column), a note after a list, a block
       * moved whole. No label is better than one over what it doesn't head
       * (DECISIONS.md 507, 508 and 510).
       */
      const label = (at: number): void => {
        if (!CARRIES_ON.has(kids[at]!.tagName)) return;
        const heading = kids.slice(0, at).filter(visible).at(-1);
        if (!heading || !isLabel(heading)) return;
        const copy = heading.cloneNode(true) as HTMLElement;
        // Ours, not the slide's: the layout check doesn't count it as content.
        copy.setAttribute("data-carried-label", "");
        // A reference tag that ends a heading stays at its end, after the mark.
        const ref = copy.lastElementChild?.hasAttribute("data-ref") ? copy.lastElementChild : null;
        ref?.remove();
        if (!/, continued$/.test(copy.textContent ?? "")) copy.append(", continued");
        if (ref) copy.append(ref);
        dest.prepend(copy);
      };
      const moveFrom = (i: number): void => {
        // A heading doesn't stay at the foot of a page without what it heads.
        let from = headedFrom(i);
        while (from > 0 && isHeading(kids[from - 1]!) && kids.slice(0, from - 1).some((k) => !isHeading(k))) from--;
        for (const k of kids.slice(from)) dest.appendChild(k);
        if (kids.slice(from).length) state.moved = true;
      };
      if (inColumns(container) && sideBySide(kids)) {
        // Taking each crossing item on alone would leave those between behind, out of
        // order. Items go from the end, whole and kept in order, until what is left fits:
        // the browser balances it and we ask whether it does. One stays when nothing else
        // has stayed on the page (DECISIONS.md 580), and an item that takes no room is never
        // the one that stays (577).
        let keep = kids.length;
        while (keep > 0 && kids.slice(0, keep).some((k) => bottomOf(k) > limit)) {
          if (!state.placed && kids.slice(0, keep - 1).every(noRoom)) break;
          dest.prepend(kids[--keep]!);
          state.moved = true;
        }
        if (kids.slice(0, keep).some((k) => !isHeading(k) && !noRoom(k))) state.placed = true;
        return;
      }
      if (sideBySide(kids)) {
        // Each crossing child goes on alone; the others in its row stay.
        for (const kid of kids) {
          if (noRoom(kid)) continue;
          if (bottomOf(kid) <= limit) {
            if (!isHeading(kid)) state.placed = true;
            continue;
          }
          if (splittable(kid) && box(kid).top < limit) {
            const inner = shell(kid);
            dest.appendChild(inner);
            breakAt(kid, inner, limit, state);
            if (kidsOf(kid).length === 0) kid.remove();
          } else if (state.placed) {
            dest.appendChild(kid);
            state.moved = true;
          }
        }
        return;
      }
      for (let i = 0; i < kids.length; i++) {
        const kid = kids[i]!;
        if (!visible(kid) || bottomOf(kid) <= limit) {
          if (!isHeading(kid) && !noRoom(kid)) state.placed = true;
          continue;
        }
        // Moved alone, it would make a slide of only its heading (DECISIONS.md 576).
        if (noRoom(kid)) continue;
        if (splittable(kid) && box(kid).top < limit) {
          const inner = shell(kid);
          dest.appendChild(inner);
          const was = state.moved;
          // Whether anything stays above it, before its own children are counted.
          const above = state.placed;
          breakAt(kid, inner, limit, state);
          // Nothing inside it crossed: its own margin or padding did. It moves whole when
          // something stays above it. With nothing above, the next slide would be this one
          // again, so it stays as the slide's content and what follows goes on (DECISIONS.md 574).
          // Nothing moved but its columns left their empty shells: it stays as a grid always
          // did, whatever is above it, the shells go, and what follows goes on by the rules
          // below, which record what they move. Moved unrecorded, it was lost (576).
          if (state.moved === was && onlyShells(inner)) {
            const shells = kidsOf(inner).length > 0;
            inner.remove();
            if (!shells) {
              state.placed = above;
              if (state.placed) {
                moveFrom(i);
                return;
              }
            }
            state.placed = true;
            continue;
          }
          for (const k of kids.slice(i + 1)) dest.appendChild(k);
          if (kidsOf(kid).length === 0) {
            // None of it stayed, so it went whole, and the headings over it go with it.
            kid.remove();
            for (const k of kids.slice(headedFrom(i), i)) dest.insertBefore(k, inner);
          } else label(i);
          return;
        }
        if (TEXT_TAGS.has(kid.tagName) && box(kid).top < limit) {
          const tail = splitText(kid, limit);
          if (tail) {
            dest.appendChild(tail);
            state.moved = true;
            state.placed = true;
            for (const k of kids.slice(i + 1)) dest.appendChild(k);
            label(i);
            return;
          }
        }
        // A row too tall to move whole, such as a long definition, breaks inside its longest cell.
        const pageTop = box(container.closest(".page") ?? container).top;
        if (kid.tagName === "TR" && box(kid).top < limit && (!state.placed || box(kid).height > (limit - pageTop) / 2)) {
          const cells = [...kid.children] as HTMLElement[];
          const longest = cells.reduce((a, b) => ((b.textContent ?? "").length > (a.textContent ?? "").length ? b : a));
          const tail = splitText(longest, limit);
          if (tail) {
            const row = kid.cloneNode(false) as HTMLElement;
            for (const cell of cells) row.appendChild(cell === longest ? tail : cell.cloneNode(false));
            dest.appendChild(row);
            state.moved = true;
            state.placed = true;
            for (const k of kids.slice(i + 1)) dest.appendChild(k);
            return;
          }
        }
        if (state.placed) moveFrom(i);
        return;
      }
    };

    /**
     * The continued page: the same kind of page, its eyebrow and title marked as
     * continued, then the moved content. The copies are ours, as a copied label is,
     * so the layout check doesn't count them as the page's content (DECISIONS.md 570).
     */
    const continuation = (page: HTMLElement): HTMLElement => {
      const next = shell(page);
      for (const kid of kidsOf(page)) {
        if (kid.classList.contains("eyebrow")) {
          const eyebrow = kid.cloneNode(true) as HTMLElement;
          eyebrow.setAttribute("data-carried-label", "");
          next.appendChild(eyebrow);
        } else if (kid.tagName === "H1" || kid.tagName === "H2") {
          const title = kid.cloneNode(true) as HTMLElement;
          title.setAttribute("data-carried-label", "");
          if (!/, continued$/.test(title.textContent ?? "")) title.append(", continued");
          next.appendChild(title);
          break;
        } else break;
      }
      return next;
    };

    const pages = (): HTMLElement[] => [...document.querySelectorAll<HTMLElement>(".page")];
    let added = 0;
    for (let i = 0; i < pages().length && added < MAX_ADDED; i++) {
      const page = pages()[i]!;
      if (page.classList.contains("cover") || !overflows(page)) continue;
      // Centred content spills off the top too, which would put the break in
      // the wrong place. A full page looks the same either way, so it starts at the top.
      page.style.justifyContent = "flex-start";
      const limit = limitOf(page);
      const next = continuation(page);
      const headed = next.children.length;
      const state: State = { placed: false, moved: false };
      // The heading is part of the page, not of what moves.
      page.after(next);
      const body = document.createElement("div");
      breakAt(page, body, limit, state);
      // With nothing recorded as moved the body is only empty shells, copies of what stays.
      // Anything printed in it has left the slide, so it goes on, recorded or not: a slide
      // gives up nothing that isn't printed somewhere (DECISIONS.md 576).
      if (body.children.length === 0 || (!state.moved && !printed(body))) {
        next.remove();
        page.style.justifyContent = "";
        continue;
      }
      while (body.firstChild) next.appendChild(body.firstChild);
      const footer = page.querySelector(":scope > [data-chrome]");
      if (footer) next.appendChild(footer.cloneNode(true));
      // A list that carries on keeps counting. One that opens with the rest of a cut
      // item starts one lower: the rest holds, unseen, the number of the item it
      // belongs to, so the item after it keeps its own (DECISIONS.md 572).
      for (const list of next.querySelectorAll<HTMLOListElement>("ol")) {
        const source = [...page.querySelectorAll<HTMLOListElement>("ol")].find((o) => o.className === list.className);
        const rest = list.firstElementChild?.hasAttribute("data-carried-rest") ? 1 : 0;
        if (source) list.start = (source.start || 1) + source.querySelectorAll(":scope > li").length - rest;
      }
      if (next.children.length <= headed) next.remove();
      else added++;
    }

    // A copy nothing went into would print an empty panel or box, or a table's head over no rows.
    // It is hidden, not removed: it holds its column's place, and the column beside it would move
    // left (DECISIONS.md 577). One that prints nothing of its own is left as it is.
    for (const el of document.querySelectorAll<HTMLElement>(".page *")) if (made.has(el) && onlyShells(el) && marks(el)) el.style.visibility = "hidden";

    // Number every page again, and point the contents at where each part now starts.
    const all = pages();
    const now = new Map<string, number>();
    all.forEach((p, i) => {
      if (p.dataset.page) now.set(p.dataset.page, i + 1);
    });
    for (const pg of document.querySelectorAll<HTMLElement>("[data-goto]")) {
      const at = now.get(pg.dataset.goto ?? "");
      if (at !== undefined) pg.textContent = String(at);
    }
    all.forEach((p, i) => {
      p.dataset.page = String(i + 1);
      const number = p.querySelector(":scope > [data-chrome] > span:last-child");
      if (number && /^\d+$/.test((number.textContent ?? "").trim())) number.textContent = String(i + 1);
    });
    return added;
  });
}
