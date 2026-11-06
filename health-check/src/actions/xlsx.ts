import { strToU8, zipSync } from "fflate";

// A small writer for the action list workbook (DECISIONS.md 191): text and
// number cells, a bold frozen header with a filter, column widths, links out
// to Salesforce and links between sheets. It's Office Open XML zipped with
// fflate, already a dependency, rather than a spreadsheet library: we need a
// tenth of what one does, and every package added is one more to trust. Text
// is always written as text, never as a formula, so a record named "=SUM(...)"
// stays a name.

export type Cell = string | number | null | { text: string; url: string } | { text: string; sheet: string };
export type Column = { header: string; width: number };
export type Sheet = {
  /** At most 31 characters, none of []:*?/\ */
  name: string;
  /** Lines above the header, such as "the first 5,000 of 7,210". */
  notes?: string[];
  columns: Column[];
  rows: Cell[][];
};

const MAX_CELL = 32_767;
const FORBIDDEN_NAME = /[[\]:*?/\\]/;

const escape = (text: string): string =>
  text
    // Characters XML 1.0 can't hold at all.
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f￾￿]/g, "")
    .slice(0, MAX_CELL)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

export function columnName(index: number): string {
  let name = "";
  for (let n = index + 1; n > 0; n = Math.floor((n - 1) / 26)) name = String.fromCharCode(65 + ((n - 1) % 26)) + name;
  return name;
}

// Styles by index: 0 plain, 1 bold header, 2 link, 3 a note, 4 wrapped text.
const STYLES = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="4"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font><font><u/><sz val="11"/><color rgb="FF0563C1"/><name val="Calibri"/></font><font><i/><sz val="11"/><color rgb="FF595959"/><name val="Calibri"/></font></fonts>
<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="5"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/><xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0" applyFont="1"/><xf numFmtId="0" fontId="3" fillId="0" borderId="0" xfId="0" applyFont="1"/><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment wrapText="1" vertical="top"/></xf></cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>`;

const text = (ref: string, value: string, style = 0): string => `<c r="${ref}" t="inlineStr"${style ? ` s="${style}"` : ""}><is><t xml:space="preserve">${escape(value)}</t></is></c>`;

function worksheet(sheet: Sheet): { xml: string; links: string[] } {
  const links: string[] = [];
  const hyperlinks: string[] = [];
  const top = sheet.notes?.length ?? 0;
  const header = top + 1;
  const last = header + sheet.rows.length;
  const lines: string[] = [];
  (sheet.notes ?? []).forEach((note, i) => lines.push(`<row r="${i + 1}">${text(`A${i + 1}`, note, 3)}</row>`));
  lines.push(`<row r="${header}">${sheet.columns.map((c, i) => text(`${columnName(i)}${header}`, c.header, 1)).join("")}</row>`);
  sheet.rows.forEach((row, r) => {
    const n = header + 1 + r;
    const cells = row.map((cell, i) => {
      const ref = `${columnName(i)}${n}`;
      if (cell === null || cell === "") return "";
      if (typeof cell === "number") return `<c r="${ref}"><v>${cell}</v></c>`;
      if (typeof cell === "string") return text(ref, cell, 4);
      if ("url" in cell) {
        links.push(cell.url);
        hyperlinks.push(`<hyperlink ref="${ref}" r:id="rId${links.length}"/>`);
      } else {
        hyperlinks.push(`<hyperlink ref="${ref}" location="${escape(`'${cell.sheet}'!A1`)}" display="${escape(cell.text)}"/>`);
      }
      return text(ref, cell.text, 2);
    });
    lines.push(`<row r="${n}">${cells.join("")}</row>`);
  });
  const end = columnName(sheet.columns.length - 1);
  const xml = [
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`,
    `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">`,
    `<sheetViews><sheetView workbookViewId="0"><pane ySplit="${header}" topLeftCell="A${header + 1}" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>`,
    `<cols>${sheet.columns.map((c, i) => `<col min="${i + 1}" max="${i + 1}" width="${c.width}" customWidth="1"/>`).join("")}</cols>`,
    `<sheetData>${lines.join("")}</sheetData>`,
    `<autoFilter ref="A${header}:${end}${Math.max(last, header)}"/>`,
    hyperlinks.length ? `<hyperlinks>${hyperlinks.join("")}</hyperlinks>` : "",
    `</worksheet>`,
  ].join("");
  return { xml, links };
}

export function writeXlsx(sheets: Sheet[]): Uint8Array {
  if (sheets.length === 0) throw new Error("A workbook needs a sheet");
  const names = new Set<string>();
  for (const s of sheets) {
    if (!s.name || s.name.length > 31 || FORBIDDEN_NAME.test(s.name) || names.has(s.name.toLowerCase())) throw new Error(`"${s.name}" can't be a sheet name`);
    names.add(s.name.toLowerCase());
  }
  const files: Record<string, Uint8Array> = {};
  const put = (path: string, content: string): void => void (files[path] = strToU8(content));
  put(
    "[Content_Types].xml",
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>${sheets.map((_, i) => `<Override PartName="/xl/worksheets/sheet${i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`).join("")}</Types>`,
  );
  put("_rels/.rels", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`);
  const filters = sheets.map((s, i) => {
    const header = (s.notes?.length ?? 0) + 1;
    return `<definedName name="_xlnm._FilterDatabase" localSheetId="${i}" hidden="1">${escape(`'${s.name}'!$A$${header}:$${columnName(s.columns.length - 1)}$${Math.max(header + s.rows.length, header)}`)}</definedName>`;
  });
  put(
    "xl/workbook.xml",
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>${sheets.map((s, i) => `<sheet name="${escape(s.name)}" sheetId="${i + 1}" r:id="rId${i + 1}"/>`).join("")}</sheets><definedNames>${filters.join("")}</definedNames></workbook>`,
  );
  put(
    "xl/_rels/workbook.xml.rels",
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">${sheets.map((_, i) => `<Relationship Id="rId${i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${i + 1}.xml"/>`).join("")}<Relationship Id="rId${sheets.length + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`,
  );
  put("xl/styles.xml", STYLES);
  sheets.forEach((sheet, i) => {
    const { xml, links } = worksheet(sheet);
    put(`xl/worksheets/sheet${i + 1}.xml`, xml);
    if (links.length) {
      put(
        `xl/worksheets/_rels/sheet${i + 1}.xml.rels`,
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">${links.map((url, k) => `<Relationship Id="rId${k + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink" Target="${escape(url)}" TargetMode="External"/>`).join("")}</Relationships>`,
      );
    }
  });
  // A fixed date, so the same workbook zips to the same bytes.
  const mtime = new Date("2026-01-01T00:00:00Z");
  return zipSync(Object.fromEntries(Object.entries(files).map(([path, data]) => [path, [data, { mtime }]])) as never);
}
