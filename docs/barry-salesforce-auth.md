# Barry's Salesforce connection

The worker (`worker/worker.ts` → `worker/salesforce.ts`) calls nine Apex REST endpoints under `/services/apexrest/barry/*` (case creation, lookups, CSAT, etc.). It authenticates with **OAuth 2.0 Client Credentials Flow** — no refresh token, nothing that expires on its own.

## Why this instead of a refresh token

The previous setup used a refresh token tied to a personal Salesforce login. Those die whenever that person's password resets, a session gets revoked, or an org security policy expires it — with no warning, and no way to tell it happened except everything silently failing (which is what happened: see the `invalid_grant: expired access/refresh token` failures in the queue on 2026-09-27).

Client Credentials Flow runs as a dedicated **Integration User** instead. The worker mints a fresh access token from a Client ID and Secret on every request — there's no long-lived secret stored anywhere that a personal account's lifecycle can invalidate.

## What's granted

Permission set **Barry Integration User** (`Blackcloud-sf-org/force-app/main/default/permissionsets/Barry_Integration_User.permissionset-meta.xml`) grants exactly:
- The 9 Apex REST classes Barry calls (`BarryValidateUser`, `BarryCreateContact`, `BarryGetCases`, `BarryCreateCase`, `BarryCloseCase`, `BarrySaveCsat`, `BarrySaveCsatFeedback`, `BarryUpdateCase`, `BarryAddCaseComment`)
- API Enabled

Nothing else. This is intentionally less than the previous personal-login flow had — those Apex classes run in system context (no `WITH SECURITY_ENFORCED`), so no extra object/field permissions are needed, and the org's sharing model on Account/Contact/Case is fully open (`ReadWrite` / `ControlledByParent` / `ReadWriteTransfer`), so a low-privilege user sees the same records these classes already returned before.

## Environment variables

- `SF_CLIENT_ID`, `SF_CLIENT_SECRET`, `SF_LOGIN_URL` (as before)
- `SF_REFRESH_TOKEN` — **no longer used, safe to remove**

## One-time Salesforce setup

See the steps given alongside this change — creating the Integration User, assigning the permission set, and enabling Client Credentials Flow on the connected app.
