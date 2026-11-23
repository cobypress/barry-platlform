# Black Cloud Barometer engine

The Salesforce health check engine. Read `docs/ARCHITECTURE.md` first, and `docs/DECISIONS.md` before changing an approach.

The build is run by an orchestrator and its agents: `docs/orchestration.md` is the method (start it with `claude --model fable`, then `/coordinate`).

## Commands

```bash
npm test            # all tests, no Salesforce org needed
npm run typecheck
npm run fixtures    # re-record after changing a query or the synthetic org
npm run hc -- run --org <alias> --calibration <file>
npm run hc -- facts --org <alias>   # only the Salesforce names we assume, never records
npm run questions -- <facts.json | run folder ...>   # which QUESTIONS.md items real orgs settle
```

Node 24 runs the TypeScript directly, so there's no build. Use `.ts` extensions in imports, and only erasable syntax: no enums or parameter properties.

## Hard rules

1. Code calculates, Claude explains. Claude may not introduce a figure.
2. Every finding has evidence, severity, confidence, value, effort, owner and dependencies.
3. Remove, deactivate and merge recommendations pass guards first. Unknown blocks.
4. Segment before concluding. Use `segmentRate` in `src/engine/rule.ts`.
5. If data can't support a metric, return not measurable with the reason. Never estimate.
6. Salesforce access is read-only. The engine never changes an org, not even to fix what it finds: every fix is a recommendation a person carries out. What can't be read becomes a manual step for a person on a consultant-led run, and is reported as not measured, with the reason, on an automatic run. Don't widen `src/salesforce/allowlist.ts` without a test for what you added and a line in DECISIONS.md. Don't add a generic request method to the client.
7. CRM records stay in the worker. Use `ctx.userLabel()` and never put a name in a finding.
8. No customer data in this repo, including fixtures. Nothing from a real org, ever. Fixture ids are zero-padded and emails use example.com.
9. Respect the API budget. Add new extracts as resumable steps.
10. Nothing reaches a customer unchecked. A consultant-led run is released only when every section is approved, with the approver's name and time. An automatic run is released by code, recorded as automatic with no approver and never a made-up one, only when every figure in what the customer reads traces to the run's data, words from the customer's org are marked apart from ours, a draft that fails its check has fallen back to our own wording, a detail that cannot be traced is withheld, and nothing is held for us. A run that is held waits for a named person. A person reads a sample of automatic runs afterwards.
11. Benchmark data has no names, org ids or link back to a customer.

## Working here

- Never run anything against a customer org. `HC_APPROVED_ORG_ALIASES` stays empty unless Coby says otherwise.
- Don't open or print `.env` files. Add new variables to DECISIONS.md.
- Builders don't push or deploy. The orchestrator merges and deploys once a lane has had its independent review.
- Salesforce facts go wrong quietly. If a rule depends on one you can't verify, say so in QUESTIONS.md.
- Report text is plain UK English, in a we and you voice, with no emoji.
- Comments explain why. Match the density of the file you're in.

## Adding a rule

Add it to the module in `src/domains/<domain>/index.ts`. Return `issue`, `pass` or `not_measurable`. Handle `status !== "ok"` on every source. Give a pass its `segmentsChecked`, or a `segmentExemptReason`. Then plant a trap for it in `src/testing/synthetic-org.ts`, run `npm run fixtures`, and test that it fires and that it stays quiet on clean data.

## Adding a query

Run it through an extractor with `readSoql`, so failure becomes `unavailable`. Never query inside a rule. If a Salesforce filter fails on a field, add the field to `UNFILTERABLE` in `src/salesforce/soql.ts`.
