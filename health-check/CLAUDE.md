# Black Cloud Barometer engine

The Salesforce health check engine. Read `docs/ARCHITECTURE.md` first, and `docs/DECISIONS.md` before changing an approach.

## Commands

```bash
npm test            # all tests, no Salesforce org needed
npm run typecheck
npm run fixtures    # re-record after changing a query or the synthetic org
npm run hc -- run --org <alias> --calibration <file>
```

Node 24 runs the TypeScript directly, so there's no build. Use `.ts` extensions in imports, and only erasable syntax: no enums or parameter properties.

## Hard rules

1. Code calculates, Claude explains. Claude may not introduce a figure.
2. Every finding has evidence, severity, confidence, value, effort, owner and dependencies.
3. Remove, deactivate and merge recommendations pass guards first. Unknown blocks.
4. Segment before concluding. Use `segmentRate` in `src/engine/rule.ts`.
5. If data can't support a metric, return not measurable with the reason. Never estimate.
6. Salesforce access is read-only. Don't widen `src/salesforce/allowlist.ts` without a test for what you added and a line in DECISIONS.md. Don't add a generic request method to the client.
7. CRM records stay in the worker. Use `ctx.userLabel()` and never put a name in a finding.
8. No customer data in this repo, including fixtures. Nothing from a real org, ever. Fixture ids are zero-padded and emails use example.com.
9. Respect the API budget. Add new extracts as resumable steps.
10. Nothing is released until every section is approved, with the approver's name and time.
11. Benchmark data has no names, org ids or link back to a customer.

## Working here

- Never run anything against a customer org. `HC_APPROVED_ORG_ALIASES` stays empty unless Coby says otherwise.
- Don't open or print `.env` files. Add new variables to DECISIONS.md.
- Coby handles pushes. Don't push or deploy.
- Salesforce facts go wrong quietly. If a rule depends on one you can't verify, say so in QUESTIONS.md.
- Report text is plain UK English, in a we and you voice, with no emoji.
- Comments explain why. Match the density of the file you're in.

## Adding a rule

Add it to the module in `src/domains/<domain>/index.ts`. Return `issue`, `pass` or `not_measurable`. Handle `status !== "ok"` on every source. Give a pass its `segmentsChecked`, or a `segmentExemptReason`. Then plant a trap for it in `src/testing/synthetic-org.ts`, run `npm run fixtures`, and test that it fires and that it stays quiet on clean data.

## Adding a query

Run it through an extractor with `readSoql`, so failure becomes `unavailable`. Never query inside a rule. If a Salesforce filter fails on a field, add the field to `UNFILTERABLE` in `src/salesforce/soql.ts`.
