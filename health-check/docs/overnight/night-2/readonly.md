# Lane A: read-only gap

Branch `hc/readonly`, worktree `worktrees/hc-readonly`. 3 to 4 November.

## Done

1. **Baseline.** Ran the engine as `bc-hc-dev-read-only` (run `hc-2026-11-03-b2413055`): 41 not measurable findings and 41 not measurable metrics. Ten findings and one metric were down to the connection: the Metadata API retrieve (`cfg.undocumented-fields`, seven `aut.*` rules, `bp.after-save-same-record-updates`, the object inventory metric) and Tooling `ApexClass` (`aut.apex-coverage`). The report scan failed on the retrieve too, and `AuthProvider`'s INVALID_TYPE was quietly read as "not in this org".
2. **Probes.** `scripts/probe-read-only.ts` loads the stored connection through `TokenStore`, sends GETs only (it refuses anything else, and never runs a report), and prints status, error code, row count and metadata names. Results are in DECISIONS.md 60.
3. **Test metadata on `bc-hc-dev`**, as the admin, target checked before every deploy: `scripts/dev-test-metadata` holds a custom object `BT_Widget__c` (two fields, two validation rules, a record type), `Account.BT_Region__c`, an after-save flow on Account (hardcoded id and email, same-record update, no fault path, API 55), a before-save flow on the widget, a Workflow Rule with a field update, an Apex trigger, handler and test, and a public report folder with two reports. The Metadata API still accepted a new Workflow Rule. The Apex test's query said "No such column 'BT_Region__c'" until it used `WITH SYSTEM_MODE`: a field deployed without profile permissions isn't visible to the admin either.
4. **Engine** (DECISIONS.md 73 and 74):
   - the metadata step falls back to Tooling item by item when the retrieve answers INSUFFICIENT_ACCESS (`src/extract/metadata-tooling.ts`);
   - report definitions fall back to the Analytics API's report describe. This is the one allowlist addition, with tests;
   - Apex classes read through REST, custom objects through `EntityDefinition`;
   - the private report count only trusts COUNT() after a one-row probe works;
   - `AuthProvider` INVALID_TYPE now leaves the run-as scan incomplete;
   - `aut.apex-coverage` (v2) no longer reports a failed read as "tests haven't been run";
   - field and user reference guards read unknown while the bundle lists kinds it couldn't search.
5. **Simulator.** `new Simulator({ readOnly: true })` answers as `bc-hc-dev` did, and its Tooling view and report describes are built from the same synthetic files the retrieve returns. `src/extract/__tests__/read-only-route.test.ts` runs the engine both ways: same flows, Workflow Rules, validation rules, fields and references, and every finding the same except Apex coverage and ownership skew.
6. **After** (run `hc-2026-11-03-e23658c8`, against the org with the test metadata). Same comparison with the old code on the same org: `hc-2026-11-03-c622d2b7`.

| Item | Before | After |
| --- | --- | --- |
| `cfg.undocumented-fields` | not measurable | issue: 20 of 22 fields undocumented |
| `aut.legacy-workflow-rules` | not measurable | issue: 1 active rule |
| `aut.legacy-process-builder` | not measurable | passed |
| `aut.mixed-automation-on-object` | not measurable | passed |
| `aut.hardcoded-ids-in-flows` | not measurable | issue: 1 of 2 flows |
| `aut.flows-without-fault-paths` | not measurable | issue: 1 of 1 |
| `aut.undocumented-automation` | not measurable | issue: 3 of 6 |
| `aut.old-api-versions` | not measurable | passed |
| `bp.after-save-same-record-updates` | not measurable | issue: 1 of 1 |
| `configuration.object_inventory` (metric) | not measurable | measured, 4 objects |
| `aut.apex-coverage` | not measurable (classes) | not measurable (coverage) |
| Findings not measurable | 41 | 32 |
| Findings and metrics not measurable | 81 | 71 |

The other 31 not measurable findings are an empty development org (no records, no forecasting, no Einstein Activity Capture), not the connection.

## Routes

| Route | Result as the read-only user |
| --- | --- |
| Metadata API retrieve | Fails: INSUFFICIENT_ACCESS, needs Modify All Data or Modify Metadata |
| Tooling `FlowDefinition`, `Flow` with `Metadata` (one row) | Works, used |
| REST `FlowDefinitionView` | Works, not used (Tooling `Flow` has the definition). `FlowVersionView` needs a filter on its parent |
| Tooling `WorkflowRule`, `WorkflowFieldUpdate` with `Metadata` | Works, used |
| Tooling `WorkflowAlert`, `WorkflowTask`, `WorkflowOutboundMessage` | List works (no `Name` column on alerts or tasks); `Metadata` not tried, no data |
| Tooling `ValidationRule` with `Metadata` | Works, used |
| Tooling `CustomField` | Fails: INVALID_TYPE |
| Tooling `FieldDefinition` (description), `EntityParticle` (help text) | Works for custom objects too, used. `InlineHelpText` isn't on `FieldDefinition` |
| Tooling `EntityDefinition` | Works, used in place of `CustomObject` |
| Tooling `CustomObject` | Fails: INVALID_TYPE |
| Tooling `RecordType` | Works (no `DeveloperName` column), used. REST `RecordType` returns nothing for custom objects the user can't read |
| REST `ApexClass` with `Body`, `ApexTrigger` | Works, used |
| Tooling `ApexClass`, `ApexTrigger`, `ApexOrgWideCoverage`, `ApexCodeCoverageAggregate` | Fail: INVALID_TYPE. REST `ApexCodeCoverageAggregate` too |
| REST `ApexTestResult` | Works, not used: results aren't coverage |
| Tooling `MetadataComponentDependency` | Works, not used: no report or workflow field update dependencies on a field |
| REST `ProcessDefinition`, `AssignmentRule` | Works, used to know whether they exist |
| Analytics `GET analytics/reports/<id>/describe` | Works, used |
| Analytics `GET analytics/reports` | Works, recently viewed only, not used |
| `Report USING SCOPE allPrivate` | COUNT() returns every visible report; rows fail INVALID_QUERY_SCOPE |
| REST `AuthProvider` | Fails: INVALID_TYPE |
| REST describe of a custom object without read | Fails: NOT_FOUND |
| Tooling `PlatformEventSubscriberConfig.UserId` | Fails: INVALID_FIELD. Left as it is: already marked incomplete |

## Permission set

No change. Nothing found would open coverage, private reports or auth providers without a write permission or View All Data. No package version is needed for this lane.

## Decisions

- The retrieve stays first, and Tooling is the fallback, because Tooling costs a call per item (DECISIONS.md 6).
- Item reads cap at 2,000 and report describes at 3,000, our judgement. Past the caps the gap is declared, never filled in.
- Approval processes and assignment rules can't be read through Tooling, so their existence makes reference guards unknown. Most orgs have assignment rules, so on a read-only connection field removal and user deactivation stay blocked (they already were, by private reports).
- `MetadataComponentDependency` isn't used, because it missed report and workflow field update references on `bc-hc-dev`.

## Review fixes

The coordinator's review found two gaps, both fixed with tests that failed first:

- Workflow Rules or validation rules the Tooling route couldn't read whole only went into `unsearched`, which only guards read, so `aut.legacy-workflow-rules` could pass on nothing. The bundle now has `unread`, and the rules and the inventory metric that count those kinds are not measurable while it's set.
- A workflow action or validation formula whose `Metadata` came back null counted as searched. It now goes to `unsearched`, so reference guards read unknown.

## Left, and doubts

- QUESTIONS.md 34 to 38 (28 to 32 on `hc/readonly`): paging of `EntityDefinition` and friends at scale, alert and task `Metadata`, `AuthProvider` in every org, report describe on hidden report types, and the allPrivate behaviour elsewhere.
- Not tried: a Process Builder, approval processes, a private report owned by another user, and a large org.
- The test metadata is still deployed in `bc-hc-dev`. Remove it with a destructive deploy if it gets in the way.
- DECISIONS rows 68 and 69 became 73 and 74 at merge.
- No SQL files, no new environment variables.
