import { colour, font, radius } from "../design/tokens.ts";
import { diagramObjects, sharingLabel, type OrgDocument } from "../domains/configuration/documented.ts";
import type { Chart } from "./charts.ts";
import { escapeXml } from "./format.ts";
import type { Render } from "./html.ts";

// "Your org, documented" as HTML: the diagram, and the tables the full report
// and the microsite share. The decks show the busiest objects; the raw notes and
// the microsite have every row. Every figure is a metric's segment or stat, so
// the number check traces each one (DECISIONS.md 116).

const count = (n: number): string => n.toLocaleString("en-GB");
export const DIAGRAM_LIMIT = 12;
export const SLIDE_ROWS = 12;

// ─── The data model diagram ──────────────────────────────────────────────────

const BOX_W = 150;
const BOX_H = 52;
const GAP_X = 34;
const GAP_Y = 30;
const PAD = 20;

type Cell = { row: number; col: number };

/**
 * Grid cells for each object, so related objects sit side by side. The object
 * with the most relationships goes in the middle, then each object is placed,
 * breadth first from there, in the free cell nearest the one it's related to.
 * Objects with no relationship fill what's left, busiest first. Every tie breaks
 * on the order given, so the same objects always land in the same cells.
 */
export function placeObjects(names: string[], links: Array<{ from: string; to: string }>, cols: number, rows: number): Map<string, Cell> {
  const neighbours = new Map(names.map((n) => [n, [] as string[]]));
  for (const l of links) {
    if (l.from === l.to || !neighbours.has(l.from) || !neighbours.has(l.to)) continue;
    if (!neighbours.get(l.from)!.includes(l.to)) neighbours.get(l.from)!.push(l.to);
    if (!neighbours.get(l.to)!.includes(l.from)) neighbours.get(l.to)!.push(l.from);
  }
  const rank = new Map(names.map((n, i) => [n, i]));
  const byRank = (a: string, b: string): number => rank.get(a)! - rank.get(b)!;
  const free: Cell[] = [];
  for (let row = 0; row < rows; row++) for (let col = 0; col < cols; col++) free.push({ row, col });
  const placed = new Map<string, Cell>();
  const take = (name: string, near: Cell): void => {
    const distance = (c: Cell): number => (c.row - near.row) ** 2 + (c.col - near.col) ** 2;
    const best = free.reduce((a, b) => (distance(b) < distance(a) ? b : a));
    free.splice(free.indexOf(best), 1);
    placed.set(name, best);
  };
  const middle: Cell = { row: Math.floor((rows - 1) / 2), col: Math.floor((cols - 1) / 2) };
  const hubs = [...names].sort((a, b) => neighbours.get(b)!.length - neighbours.get(a)!.length || byRank(a, b));
  for (const hub of hubs) {
    if (placed.has(hub) || neighbours.get(hub)!.length === 0) continue;
    take(hub, placed.size === 0 ? middle : { row: rows - 1, col: 0 });
    const queue = [hub];
    while (queue.length) {
      const at = queue.shift()!;
      for (const next of [...neighbours.get(at)!].sort(byRank)) {
        if (placed.has(next)) continue;
        take(next, placed.get(at)!);
        queue.push(next);
      }
    }
  }
  for (const name of names) if (!placed.has(name)) take(name, { row: 0, col: 0 });
  return placed;
}

/**
 * Objects as boxes on a grid (`placeObjects`), with a line for each
 * relationship between two of them. No layout engine: the
 * same document always draws the same picture. A master-detail line is solid
 * and heavier, a lookup dashed, so the two don't rely on colour.
 */
export function dataModelDiagram(doc: OrgDocument, columns = 3): Chart {
  const id = "data-model";
  const title = "The data model";
  if (!doc.model || doc.model.objects.length === 0) return { kind: "gap", id, title, reason: "The objects couldn't be read." };
  const { objects, links } = diagramObjects(doc, DIAGRAM_LIMIT);
  const cols = Math.min(columns, objects.length);
  const rows = Math.ceil(objects.length / cols);
  const width = PAD * 2 + cols * BOX_W + (cols - 1) * GAP_X;
  const height = PAD * 2 + rows * BOX_H + (rows - 1) * GAP_Y;
  const cells = placeObjects(objects.map((o) => o.name), links, cols, rows);
  const at = new Map([...cells].map(([name, c]) => [name, { x: PAD + c.col * (BOX_W + GAP_X), y: PAD + c.row * (BOX_H + GAP_Y) }]));
  const centre = (name: string) => ({ x: at.get(name)!.x + BOX_W / 2, y: at.get(name)!.y + BOX_H / 2 });
  // Each pair once, master-detail winning, so two lookups between the same objects draw one line.
  const pairs = new Map<string, "lookup" | "master-detail">();
  for (const l of links) {
    const key = [l.from, l.to].sort().join("|");
    if (pairs.get(key) !== "master-detail") pairs.set(key, l.kind);
  }
  const lines = [...pairs]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, kind]) => {
      const [a, b] = key.split("|") as [string, string];
      const [p, q] = [centre(a), centre(b)];
      return `<line x1="${p.x}" y1="${p.y}" x2="${q.x}" y2="${q.y}" stroke="${kind === "master-detail" ? colour.chartreuse : colour.panelMuted}" stroke-width="${kind === "master-detail" ? 3 : 1.5}"${kind === "lookup" ? ' stroke-dasharray="5 4"' : ""}/>`;
    })
    .join("");
  const boxes = objects
    .map((o) => {
      const { x, y } = at.get(o.name)!;
      const name = o.name.length > 20 ? `${o.name.slice(0, 19)}…` : o.name;
      const records = o.records === null ? "Not counted here" : `${count(o.records)} records`;
      return `<rect x="${x}" y="${y}" width="${BOX_W}" height="${BOX_H}" rx="${radius.small}" fill="${colour.carbon}" stroke="${colour.ivory}" stroke-width="1.2"/><text x="${x + BOX_W / 2}" y="${y + 22}" font-size="13" font-weight="${font.weights.semibold}" fill="${colour.ivory}" text-anchor="middle">${escapeXml(name)}</text><text x="${x + BOX_W / 2}" y="${y + 40}" font-size="11" fill="${colour.panelMuted}" text-anchor="middle">${escapeXml(records)}</text>`;
    })
    .join("");
  const alt = `The ${objects.length} busiest objects and the relationships between them.`;
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeXml(alt)}" font-family="${escapeXml(font.stack)}" data-chart="${id}"><rect width="${width}" height="${height}" rx="${radius.medium}" fill="${colour.carbon}"/>${lines}${boxes}</svg>`;
  return { kind: "chart", id, title, alt, svg };
}

export const DIAGRAM_KEY = "A solid line is master-detail, a dashed line a lookup.";

// ─── Tables ──────────────────────────────────────────────────────────────────

const table = (head: string[], rows: string[][], numeric: number[] = []): string =>
  `<table class="grid data"><thead><tr>${head.map((h, i) => `<th${numeric.includes(i) ? ' class="num"' : ""}>${h}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${row.map((c, i) => `<td${numeric.includes(i) ? ' class="num"' : ""}>${c}</td>`).join("")}</tr>`).join("")}</tbody></table>`;

const more = (r: Render, shown: number, all: number): string => (all > shown ? `<p class="small-note">${r.t("The full list is in the raw notes.")}</p>` : "");

export function objectsTable(r: Render, doc: OrgDocument, limit = Infinity): string {
  if (!doc.model) return "";
  const objects = doc.model.objects.slice(0, limit);
  const rows = objects.map((o) => [
    `${r.t(o.name)}`,
    o.records === null ? r.t("Not counted here") : count(o.records),
    o.customFields === null ? r.t("Not read") : count(o.customFields),
    o.recordTypes.filter((t) => t.active).map((t) => r.t(t.name)).join(", ") || r.t("None"),
  ]);
  return `${table([r.t("Object"), r.t("Records"), r.t("Custom fields"), r.t("Active record types")], rows, [1, 2])}${more(r, objects.length, doc.model.objects.length)}`;
}

export function relationshipsTable(r: Render, doc: OrgDocument): string {
  if (!doc.relationships || doc.relationships.links.length === 0) return "";
  return table([r.t("From"), r.t("Field"), r.t("To"), r.t("Kind")], doc.relationships.links.map((l) => [r.t(l.from), r.t(l.field), r.t(l.to), r.t(l.kind === "master-detail" ? "Master-detail" : "Lookup")]));
}

/** Objects down the side, the steps across in the order they run, each cell active out of all. */
export function automationTable(r: Render, doc: OrgDocument, limit = Infinity): string {
  if (!doc.automation || doc.automation.objects.length === 0) return "";
  const objects = doc.automation.objects.slice(0, limit);
  const steps = [...new Map(doc.automation.objects.flatMap((o) => o.steps).map((s) => [s.key, s])).values()].sort((a, b) => (a.step ?? 99) - (b.step ?? 99) || a.label.localeCompare(b.label));
  const rows = objects.map((o) => [
    r.t(o.object),
    ...steps.map((s) => {
      const cell = o.steps.find((x) => x.key === s.key);
      return !cell ? "" : cell.active === cell.all ? count(cell.active) : `${count(cell.active)} of ${count(cell.all)}`;
    }),
  ]);
  return `${table([r.t("Object"), ...steps.map((s) => r.t(s.label))], rows, steps.map((_, i) => i + 1))}${more(r, objects.length, doc.automation.objects.length)}`;
}

/** Every item on every object, in order: the microsite's long form. */
export function automationLists(r: Render, doc: OrgDocument): string {
  if (!doc.automation) return "";
  return doc.automation.objects
    .map((o) => `<h3>${r.t(o.object)}</h3>${table([r.t("When it runs"), r.t("Name"), r.t("Active"), r.t("Description")], o.steps.flatMap((s) => s.items.map((i) => [r.t(s.label), r.t(i.name), r.t(i.active ? "Yes" : "No"), r.t(i.described === null ? "Not read" : i.described ? "Yes" : "None")])))}`)
    .join("");
}

const list = (r: Render, title: string, items: string[], limit: number): string =>
  items.length ? `<h3 class="cap">${r.t(title)}</h3><ul class="doc-list">${items.slice(0, limit).map((i) => `<li>${i}</li>`).join("")}</ul>${items.length > limit ? `<p class="small-note">${r.t("The rest are in the raw notes.")}</p>` : ""}` : "";

export function integrationsBlock(r: Render, doc: OrgDocument, limit = Infinity): string {
  const i = doc.integrations;
  if (!i) return "";
  return [
    list(r, "Connected apps", i.connectedApps.map((a) => `${r.t(a.name)}: ${r.t(a.adminApprovedOnly ? "admin-approved users only" : "users can authorise it themselves")}`), limit),
    list(r, "Named credentials", i.namedCredentials.map((c) => r.t(c)), limit),
    list(r, "Remote sites", i.remoteSites.map((s) => `${r.t(s.host)}${s.active ? "" : r.t(" (inactive)")}`), limit),
    list(r, "Integration users", i.integrationUsers.map((u) => `${r.t(u.group)}: ${count(u.users)}`), limit),
    list(r, "API logins in the last 90 days", i.apiLogins.map((a) => `${r.t(a.application)}: ${count(a.logins)} by ${count(a.users)} ${r.t(a.users === 1 ? "user" : "users")}`), limit),
  ].join("") || `<p>${r.t("No connected apps, named credentials or remote sites.")}</p>`;
}

export function accessBlock(r: Render, doc: OrgDocument, limit = Infinity): string {
  const a = doc.access;
  if (!a) return "";
  const depth = a.roleDepth !== null && a.roleCount !== null ? `<p>${count(a.roleCount)} ${r.t(a.roleCount === 1 ? "role" : "roles")}, ${count(a.roleDepth)} ${r.t(a.roleDepth === 1 ? "level deep" : "levels deep")}.</p>` : "";
  return [
    `<p><strong>${count(a.metric.value)}</strong> ${r.t("active users")}</p>`,
    list(r, "By profile", a.profiles.map((p) => `${r.t(p.name)}: ${count(p.users)}`), limit),
    `<h3 class="cap">${r.t("Roles")}</h3>${depth}`,
    list(r, "By role", a.roles.map((p) => `${r.t(p.name)}: ${count(p.users)}`), limit),
    list(r, "Permission sets, by active users holding them", a.permissionSets.map((p) => `${r.t(p.name)}: ${count(p.users)}`), limit),
  ].join("");
}

export function sharingTable(r: Render, doc: OrgDocument): string {
  if (!doc.sharing || doc.sharing.objects.length === 0) return "";
  return table([r.t("Object"), r.t("Internal"), r.t("External")], doc.sharing.objects.map((s) => [r.t(s.object), r.t(s.internal ? sharingLabel(s.internal) : "Not set"), r.t(s.external ? sharingLabel(s.external) : "Not set")]));
}

/** Whether any part has something to show. With none, the part is left out everywhere. */
export const hasDocument = (doc: OrgDocument): boolean => Boolean(doc.model || doc.automation || doc.integrations || doc.access || doc.sharing);
