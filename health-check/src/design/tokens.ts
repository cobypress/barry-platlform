// The only place colours, type and shape are defined. Every renderer reads from
// here. No gradients, no emoji, mild rounding, flat fills. Red is for alerts only.

export const colour = {
  carbon: "#21221C",
  chartreuse: "#A8E503",
  ivory: "#FCFFF3",
  snow: "#F7F7F7",
  alert: "#F3261C",
  // The design system's neutral-600 and neutral-200.
  muted: "#555555",
  rule: "#EBEBEB",
  // The same job on a Carbon panel, where charts sit so Chartreuse has contrast.
  // The design system has no neutrals for Carbon, so these are flat tints of Ivory.
  panelMuted: "#B4B6A8",
  panelRule: "#3B3C33",
} as const;

export const font = {
  family: "Poppins",
  stack: "'Poppins', 'Helvetica Neue', Arial, sans-serif",
  weights: { regular: 400, medium: 500, semibold: 600, bold: 700 },
} as const;

export const radius = { small: 4, medium: 8 } as const;

export const page = {
  a4: { width: "210mm", height: "297mm", margin: "18mm" },
  // 16:9 at a size Chromium prints without scaling.
  slide: { width: "13.333in", height: "7.5in", margin: "0.55in" },
} as const;

/** The same tokens as CSS variables, for the PDF templates. */
export function tokensCss(): string {
  const vars = Object.entries(colour).map(([name, value]) => `--${name}: ${value};`);
  return `:root { ${vars.join(" ")} --font: ${font.stack}; --radius-s: ${radius.small}px; --radius-m: ${radius.medium}px; }`;
}
