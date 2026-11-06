import { readSoql, type Extractor, type Sourced } from "./types.ts";

// What Lightning Experience left behind: custom buttons and links that run
// JavaScript, s-controls, and Visualforce pages. Installed packages' own are
// left out, because the customer can't change them.

export type WebLinkRow = { Name: string; PageOrSobjectType: string | null; LinkType: string };
export type ApexPageRow = { Name: string; ApiVersion: number; IsAvailableInTouch: boolean | null };
export type ClassicData = { links: Sourced<WebLinkRow[]>; scontrols: Sourced<Array<{ Name: string }>>; pages: Sourced<ApexPageRow[]> };

export const classicExtractor: Extractor<ClassicData> = {
  id: "classic",
  version: 1,
  async run(ctx) {
    return {
      links: await readSoql<WebLinkRow>(ctx, "SELECT Name, PageOrSobjectType, LinkType FROM WebLink WHERE NamespacePrefix = null"),
      scontrols: await readSoql(ctx, "SELECT Name FROM Scontrol WHERE NamespacePrefix = null"),
      pages: await readSoql<ApexPageRow>(ctx, "SELECT Name, ApiVersion, IsAvailableInTouch FROM ApexPage WHERE NamespacePrefix = null"),
    };
  },
};
