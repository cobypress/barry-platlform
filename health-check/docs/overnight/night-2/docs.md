# Night 2, docs lane

Documents only. No code changed, no org contacted, no `sf` run.

## Done

- **Customer install guide**, `docs/customer/INSTALL.md`. Written for a customer's admin: what Barometer reads and doesn't, the permission set taken line by line from `BarometerInstall.cls`, what leaves the org, how to cut us off, six numbered steps, afterwards, troubleshooting, and a "Notes for Black Cloud" section to remove before sending.
- **Bulk API options note**, `docs/BULK-API.md`. For Coby.
- **QUESTIONS.md:** 14 marked mostly answered, and new rows 28 to 33 for what Salesforce's docs couldn't settle.
- Tests: 1,184 pass. Type check clean. The tree had no `node_modules`, so I ran `npm ci` first.

## Decisions

- **Integration user recommended over a full-licence user**, because it's proven (MILESTONES.md, "Web server flow, live") and can't browse the org.
- **"Install for Admins Only"**, since the package has no objects or pages.
- **IP relaxation left at Enforce.** The guide only raises it when the user's profile has login IP ranges, because Salesforce only enforces app IP restrictions then. That is documented for connected apps, and inferred for external client apps.
- **Step 6 says the approval page "may" appear.** Salesforce says pre-authorised users skip it, and that `prompt=consent` forces it. We've only seen it under self-authorise (QUESTIONS 29).
- **"Several" free Integration licences, not "five"**, because only secondary sources give the number.
- **Placeholders** for the install link, region, zero data retention and outbound IPs, each marked `[BLACK CLOUD: ...]`.

## Doubts

- **DECISIONS 59 says read on Task and Event `WhoId` and `WhatId`.** The script and the reference permission set grant Task only (QUESTIONS 32).
- **help.salesforce.com renders with JavaScript.** Some articles loaded through web fetch: OAuth scopes, pre-authorisation, idle refresh token TTL, OAuth usage, connected app OAuth policies and the web server flow. Others, such as the integration user article, wouldn't load. developer.salesforce.com answered 403, as before. The Version 68.0 PDFs were used instead: the 2GP guide, the REST guide, the Bulk API guide, the limits cheat sheet, the SOQL guide and the Metadata API guide.
- **Bulk API facts came from a research helper** reading the Version 68.0 PDFs. I checked its endpoint and limit quotes against the downloaded text.

## Left

- Fill the placeholders once a version is promoted and hosting is decided.
- Try the guide end to end on the first install of a released version. That answers QUESTIONS 28 to 31.
- No SQL files, and no new environment variables.
