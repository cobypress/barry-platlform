// Metadata can hold secrets a developer pasted in: a key in a flow constant, a
// token in Apex, a consumer secret in an app definition. Rule 7 says they're
// removed before metadata goes anywhere near Claude. Salesforce also masks some
// of these on retrieve, but that can't be relied on.

const REPLACEMENT = "[REDACTED]";

const PATTERNS: ReadonlyArray<[RegExp, string]> = [
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, REPLACEMENT],
  // An XML element whose name says it's a secret.
  [/(<([A-Za-z]*(?:password|secret|apiKey|privateKey|accessToken|refreshToken|clientSecret|consumerSecret)[A-Za-z]*)>)[^<]*(<\/\2>)/gi, `$1${REPLACEMENT}$3`],
  // key = "value" and key: 'value' in Apex, formulas and config.
  [/((?:password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|auth[_-]?token)["']?\s*[:=]\s*["'])[^"'\n]{4,}(["'])/gi, `$1${REPLACEMENT}$2`],
  [/\bBearer\s+[A-Za-z0-9._~+/!-]{16,}=*/g, `Bearer ${REPLACEMENT}`],
  [/\bAKIA[0-9A-Z]{16}\b/g, REPLACEMENT],
  [/\bsk-[A-Za-z0-9_-]{20,}\b/g, REPLACEMENT],
  [/\bxox[abprs]-[A-Za-z0-9-]{10,}\b/g, REPLACEMENT],
  [/\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/g, REPLACEMENT],
  // A Salesforce session id: org id, "!", then the token body.
  [/\b00D[A-Za-z0-9]{12,15}![A-Za-z0-9._]{20,}/g, REPLACEMENT],
];

export function redactSecrets(text: string): string {
  return PATTERNS.reduce((out, [pattern, replacement]) => out.replace(pattern, replacement), text);
}
