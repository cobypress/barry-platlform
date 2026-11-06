// What the technical health and integration checks need to know from an Apex
// body, worked out while the body is in hand: the extractor drops it straight
// after, so code never sits in a checkpoint. These are text scans, not a parser.
// They err towards missing a case rather than inventing one, and the rules that
// use them say they're a reading of the code, at medium confidence.

export type ApexScan = {
  /** A comment anywhere outside a string. "https://" in a string isn't one. */
  commented: boolean;
  /** The class itself is @isTest, so it doesn't count against the org's Apex size limit. */
  testClass: boolean;
  /** @isTest methods or testMethod, so a test data factory isn't read as a test that asserts nothing. */
  testMethods: number;
  /** System.assert, Assert.areEqual and helpers named assert-something. */
  asserts: number;
  seeAllData: boolean;
  /** SOQL or Database.query inside a loop's body. A SOQL for loop's own query isn't counted. */
  queriesInLoops: number;
  /** DML statements or Database DML methods inside a loop's body. */
  dmlInLoops: number;
  /** setEndpoint given a literal http or https address, rather than a named credential. */
  literalEndpoints: number;
  /** setEndpoint given a "callout:" named credential. */
  namedCredentialEndpoints: number;
};

// A statement, so the keyword follows a statement's end, a block or a condition. Matched on the keyword itself.
const DML = /(?<=(?:^|[;{})]|\belse)\s*)\b(?:insert|update|upsert|delete|undelete|merge)\s+[\w(\[]/gi;
const DATABASE_DML = /\bDatabase\s*\.\s*(?:insert|update|upsert|delete|undelete|merge|convertLead)\s*\(/gi;
const SOQL = /\[\s*SELECT\b/gi;
const DATABASE_QUERY = /\bDatabase\s*\.\s*(?:query|queryWithBinds|countQuery|getQueryLocator)\s*\(/gi;

const count = (text: string, pattern: RegExp): number => text.match(pattern)?.length ?? 0;

/** Comments go, and each string's contents are blanked, so every offset still points at the same place in the body. */
export function codeOnly(body: string): string {
  return read(body).code;
}

function read(body: string): { code: string; comments: number } {
  let code = "";
  let comments = 0;
  for (let i = 0; i < body.length; ) {
    const two = body.slice(i, i + 2);
    if (two === "//") {
      comments++;
      const end = body.indexOf("\n", i);
      const stop = end === -1 ? body.length : end;
      code += " ".repeat(stop - i);
      i = stop;
    } else if (two === "/*") {
      comments++;
      const end = body.indexOf("*/", i + 2);
      const stop = end === -1 ? body.length : end + 2;
      code += body.slice(i, stop).replace(/[^\n]/g, " ");
      i = stop;
    } else if (body[i] === "'") {
      let j = i + 1;
      while (j < body.length && body[j] !== "'") j += body[j] === "\\" ? 2 : 1;
      code += `'${" ".repeat(Math.max(0, Math.min(j, body.length) - i - 1))}'`;
      i = j + 1;
    } else {
      code += body[i];
      i++;
    }
  }
  return { code, comments };
}

/** The index just past the bracket that closes the one at `open`, or the end. */
function closing(code: string, open: number, pair: "()" | "{}"): number {
  let depth = 0;
  for (let i = open; i < code.length; i++) {
    if (code[i] === pair[0]) depth++;
    else if (code[i] === pair[1] && --depth === 0) return i + 1;
  }
  return code.length;
}

/** Each loop body's span. A body without braces runs to its first semicolon. */
function loopBodies(code: string): Array<[number, number]> {
  const spans: Array<[number, number]> = [];
  for (const m of code.matchAll(/\b(for|while)\s*\(/gi)) {
    const header = closing(code, m.index + m[0].length - 1, "()");
    const start = header + (code.slice(header).match(/^\s*/)?.[0].length ?? 0);
    if (code[start] === "{") spans.push([start, closing(code, start, "{}")]);
    else if (code[start] !== ";") spans.push([start, code.indexOf(";", start) === -1 ? code.length : code.indexOf(";", start) + 1]);
  }
  // A do-while's body comes first.
  for (const m of code.matchAll(/\bdo\s*\{/gi)) spans.push([m.index + m[0].length - 1, closing(code, m.index + m[0].length - 1, "{}")]);
  return spans;
}

/** Positions inside any loop body, each counted once however deeply nested. */
function inLoops(code: string, spans: Array<[number, number]>, patterns: RegExp[]): number {
  const hits = new Set<number>();
  for (const pattern of patterns) for (const m of code.matchAll(pattern)) if (spans.some(([a, b]) => m.index >= a && m.index < b)) hits.add(m.index);
  return hits.size;
}

export function scanApex(body: string): ApexScan {
  const { code, comments } = read(body);
  const spans = loopBodies(code);
  // Found in the code, then read from the body at the same offset, so an endpoint in a comment never counts.
  const endpoints = [...code.matchAll(/\bsetEndpoint\s*\(\s*'/gi)].map((m) => {
    const start = m.index + m[0].length;
    return body.slice(start, code.indexOf("'", start));
  });
  const annotatedClass = /^\s*@isTest\b(?:\s*\([^)]*\))?\s+(?:\w+\s+)*?class\b/i.test(code);
  return {
    commented: comments > 0,
    testClass: annotatedClass,
    testMethods: count(code, /@isTest\b/gi) - (annotatedClass ? 1 : 0) + count(code, /\btestMethod\b/gi),
    asserts: count(code, /\b(?:System\s*\.\s*)?assert\w*\s*\(/gi) + count(code, /\bAssert\s*\.\s*\w+\s*\(/gi),
    seeAllData: /@isTest\s*\(\s*SeeAllData\s*=\s*true\s*\)/i.test(code),
    queriesInLoops: inLoops(code, spans, [SOQL, DATABASE_QUERY]),
    dmlInLoops: inLoops(code, spans, [DML, DATABASE_DML]),
    literalEndpoints: endpoints.filter((e) => /^https?:\/\//i.test(e)).length,
    namedCredentialEndpoints: endpoints.filter((e) => /^callout:/i.test(e)).length,
  };
}
