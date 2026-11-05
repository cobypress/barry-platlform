# Connecting Salesforce to Black Cloud Barometer

Barometer is Black Cloud's health check for Salesforce. Your consultant runs it, and it needs to read your org to do that. This guide is for the Salesforce administrator who sets up that access. It takes about 20 minutes.

You'll install a small package from us and sign in once, as yourself or as any user of your org. If you'd like Salesforce itself to hold Barometer to reading as well, you can also set up a user that can only read, and connect as that user instead (steps 2 to 5, which are optional). The rest of this page explains each step and what you're agreeing to.

## What Barometer does and doesn't do

### It reads. It never changes anything

Barometer looks at how your org is set up and how it's used, and writes a report. Every fix in that report is a recommendation for a person to carry out. Barometer never creates, edits or deletes a record or a setting, not even to fix something it found.

**Our service can't ask.** The Barometer service only sends a fixed list of read requests, such as queries, object descriptions and report definitions. Anything else is stopped before it leaves our service, whoever you sign in as, an administrator included. We test this by running a whole health check as a user who could change anything, and checking that every request it sent was on the list.

**Optionally, Salesforce can refuse too.** If you connect as a user who holds only the Barometer Read Only permission set, Salesforce itself refuses any write from that user. We tested this by trying nine kinds of write as that user, including creating and deleting records, anonymous Apex and a metadata deployment. Salesforce refused all nine. Steps 2 to 5 set this up.

When you connect, we read the user's permissions from your org's own records and tell your consultant what that user could change. It doesn't stop the connection.

### What the permission set grants

The package doesn't contain the permission set itself, because Salesforce doesn't let a package carry these permissions. Instead, a short script runs once when you install the package and creates an ordinary permission set called **Barometer Read Only**. You can open it, check it and change it like any other permission set.

The script gives it these permissions and nothing else.

**System and user permissions**

| Permission | API name | Why we need it |
| --- | --- | --- |
| API Enabled | `ApiEnabled` | Barometer reads through Salesforce's API |
| View Roles and Role Hierarchy | `ViewRoles` | Needed for View Setup and Configuration |
| View Setup and Configuration | `ViewSetup` | Setup records such as profiles, permission sets, licences and the setup audit trail |
| View All Users | `ViewAllUsers` | Every user, to check licence use and ownership |
| Monitor Login History | `MonitorLoginHistory` | Logins across the org, not only this user's own |
| View Health Check | `ViewHealthCheck` | Salesforce's own Security Health Check score |
| View Reports in Public Folders | `ViewPublicReports` | Report use |
| View Dashboards in Public Folders | `ViewPublicDashboards` | Dashboard use |
| Run Reports | `RunReports` | Without it, the user can't read the list of reports at all |
| Access Activities | `ActivitiesAccess` | Tasks and events |

**Objects:** Read, View All Records and View All Fields on Account, Campaign, Case, Contact, Lead and Opportunity.

**Fields:** Read on the task's Related To (`WhatId`) and Name (`WhoId`) fields. Salesforce applies these to events too.

**What it doesn't grant:**

- No Create, Edit or Delete on any object, and no Modify All Records.
- No Modify All Data, View All Data, Modify Metadata, Customize Application, Author Apex or Manage Users.
- Nothing on objects not listed above. Custom objects stay hidden unless you choose to add read access (see "Optional: custom objects" below).

If you already have a permission set called Barometer Read Only, the script leaves it alone and creates nothing. If your edition lacks one of the permissions above, the script sets the rest and adds the missing ones to the end of the permission set's description: "Not granted: ...".

Because this is a managed package, Salesforce hides the script's code from you. The permission set it creates is the record of what it did, and your consultant can send you the script's source if you'd like to read it.

### What leaves your org, and where it goes

Barometer reads:

- **Setup and configuration:** company settings, users (name, username, email, profile, role, last login and whether they're active), licences, profiles and permission sets, login history for the last 90 days, the setup audit trail for the last 180 days, Security Health Check results, report and dashboard names and folders, report definitions (columns, groupings and filters, without running the report), the definitions of flows, workflow rules and validation rules, scheduled jobs, duplicate rules, and the code of your own Apex classes and triggers.
- **Sales records, as figures and dates:** opportunities (owner, account, stage, amount, close date, type, lead source and key dates) and their stage history, leads (owner, source, dates and conversion), campaign members, forecasts, and the dates and links of tasks, events and emails.
- **Counts only:** how full each custom field is, contacts without an account or email address, and record counts by owner.

It doesn't read the subject or body of any email, task or event, or the names of accounts and contacts.

That data goes to the Barometer service that Black Cloud runs, in [BLACK CLOUD: region and provider]. There:

- the raw records are deleted 30 days after we deliver your report. The findings, figures and the report itself are kept;
- the sign-in token for the user you connected is stored encrypted, and is only used for reads;
- to help write the report's wording, we send summaries to Anthropic's Claude model. Before anything is sent, every person's name and your company's name are replaced with codes, and no records are sent. [BLACK CLOUD: confirm zero data retention with Anthropic before this ships.]

Each run uses part of your org's daily API allowance. It stops before your org's use for the day passes half the allowance, and never makes more than 5,000 calls, so your own integrations keep the larger share.

### How to cut us off

You can stop Barometer at any time, without asking us:

- **Revoke the connection** in Setup (see "Revoke the connection" below). This takes effect at once.
- **If you connected a Barometer user, remove Barometer Read Only from it.** Without API Enabled, Salesforce refuses every request from that user.
- **Uninstall the package.** This removes our app from your org.

The connection also lapses by itself if it isn't used for 30 days, once you've set the policy in step 5.

## Before you start

**Edition.** Your org needs API access. It's on by default in Enterprise, Unlimited, Performance and Developer Edition. Professional Edition needs Salesforce's API add-on ([REST API Developer Guide, "Supported Editions and Required Permissions"](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/api_rest.pdf)). We haven't yet connected a Professional Edition org.

**Who does it.** A Salesforce administrator with the System Administrator profile, using Lightning Experience. Setting the app's policies needs the permission "View all External Client Apps, view their settings, and edit their policies" ([Preauthorize User App Access Through External Client App Policies](https://help.salesforce.com/s/articleView?id=xcloud.preauth_user_app_access_through_eca.htm&language=en_US&type=5)).

**A mailbox.** The user you create in step 3 needs an email address you can read, because Salesforce sends its password link and any sign-in codes there.

## Steps

### 1. Install the package

<!-- BLACK CLOUD: version 0.1.0-7 (04tQH00000NOKy1YAH), released on 5 November 2026. For a sandbox, use test.salesforce.com in place of login.salesforce.com. -->

1. Open this link while signed in to Salesforce as an administrator: `https://login.salesforce.com/packaging/installPackage.apexp?p0=04tQH00000NOKy1YAH`
2. If Salesforce asks for an installation key, enter the one your consultant sent you.
3. Choose **Install for Admins Only**. The package contains no objects or pages for your users, so the choice makes no practical difference.
4. Salesforce may warn that the package hasn't been through Salesforce's security review. That's expected for a private package that isn't listed on AppExchange. Tick the box to say you understand, and click **Install**.

Salesforce shows a message when the install is complete. The install options and the security review notice are described in Salesforce's [Second-Generation Managed Packaging Developer Guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/pkg2_dev.pdf) ("Permission Sets and Profile Settings in Packages", "Use a URL to Install a Second-Generation Managed Package" and "Install Notifications for Unauthorized Managed Packages").

### Optional: a user that can only read (steps 2 to 5)

Skip to step 6 if you're signing in as yourself or another existing user. These steps are for orgs that want Salesforce, as well as our service, to keep Barometer to reading.

### 2. Check that Barometer Read Only exists

1. In Setup, search for **Permission Sets** in Quick Find.
2. Open **Barometer Read Only**. Its description starts "Black Cloud Barometer health check: read only."
3. If the description also says "Not granted: ...", note what's listed and tell your consultant. The connection will still work, but some checks may be reported as not measurable.

If Barometer Read Only isn't there, see "Troubleshooting".

### 3. Create or choose the Barometer user

Use a user that does nothing except Barometer. Don't use your own user, or anyone else's.

You have two options.

| | Salesforce Integration user (recommended) | Full-licence user |
| --- | --- | --- |
| Licence | Salesforce Integration. Several are included free in Enterprise Edition and above | A Salesforce or Salesforce Platform licence you'd otherwise use for a person |
| Can sign in to Salesforce's screens | No: API only | Yes |
| Proven with Barometer | Yes. On 3 November 2026 we signed in as an Integration user, approved the app, and ran a full health check on that connection | Not tested, but nothing in the process needs it |

An Integration user can't be used to browse your org, which makes it the safer choice. Salesforce recommends one dedicated user for each integration ([Give Integration Users API Only Access](https://help.salesforce.com/s/articleView?language=en_US&id=platform.integration_user.htm&type=5)).

To create an Integration user:

1. In Setup, go to **Users**, and click **New User**.
2. Give it a clear name, such as "Barometer Read Only", and an email address you can read.
3. Set **User License** to **Salesforce Integration** and **Profile** to **Minimum Access - API Only Integrations**.
4. Save. Then, on the user's record, assign the **Salesforce API Integration** permission set licence.
5. Set a password for the user. You'll sign in as this user once, in step 6.

If you'd rather use a full-licence user, give it the most restricted profile you have, such as Minimum Access - Salesforce, so that Barometer Read Only is the only thing that grants it access.

### 4. Assign Barometer Read Only, and nothing else

1. On the user's record, go to **Permission Set Assignments** and click **Edit Assignments**.
2. Add **Barometer Read Only**, and save.

Don't add any other permission set or permission set group, or the user will be able to write, and only our service will be keeping Barometer to reading.

**Optional: custom objects.** Barometer Read Only doesn't include your custom objects, so we can't tell who owns records on them. If you'd like that included, add Read access on those objects to Barometer Read Only. Don't add Create, Edit or Delete.

### 5. Set the app's policies

When the package is installed, Salesforce gives our app its default policies: any user can approve it, and its sign-in lasts until revoked. These steps restrict it to the Barometer user, and make the connection lapse if unused.

1. In Setup, search for **External Client App Manager** in Quick Find, and open it.
2. Find **Black Cloud Barometer**. From its actions menu, choose **Edit Policies**.
3. Under **OAuth Policies**, set **Permitted Users** to **Admin approved users are pre-authorized**. Then choose **Barometer Read Only** as the permission set whose users may use the app.
4. Under **App Authorization**, set **Refresh Token Policy** to **Expire refresh token if not used for specific time**, and enter **30 days**.
5. Leave **IP Relaxation** as it is (**Enforce IP restrictions**). See below.
6. Save.

Sources: [Preauthorize User App Access Through External Client App Policies](https://help.salesforce.com/s/articleView?id=xcloud.preauth_user_app_access_through_eca.htm&language=en_US&type=5) (steps 1 to 3) and [Limit the Idle Refresh Token TTL in External Client Apps](https://help.salesforce.com/s/articleView?id=xcloud.eca_limit_idle_refresh_token_ttl.htm&language=en_US&type=5) (step 4). With step 3, only users who hold Barometer Read Only can use our app, so nobody else in your org, administrators included, can connect it by mistake. If you set this, connect as the Barometer user in step 6: you won't be able to connect as yourself.

**IP restrictions.** Salesforce only enforces IP restrictions for the app if the user's profile has login IP ranges ([Manage OAuth Access Policies for a Connected App](https://help.salesforce.com/s/articleView?id=xcloud.connected_app_manage_oauth.htm&language=en_US&type=5)). If the connected user's profile has none, which is usual, this setting makes no difference. If it does have them, talk to your consultant before connecting: our service connects from [BLACK CLOUD: outbound IP addresses], and those would need to be in the range.

### 6. Connect

Your consultant will email you a connection link. It works once, and lasts 72 hours.

1. Open the link in a private or incognito browser window, so that you aren't already signed in to Salesforce as yourself.
2. The page says who's asking and what Barometer will read. Click **Continue**.
3. Salesforce asks you to sign in. Sign in as the user you've chosen: yourself, or the Barometer user if you followed steps 2 to 5.
4. Salesforce may show a page asking you to allow Black Cloud Barometer access. It lists two permissions:
   - **Manage user data via APIs**: reading your data through Salesforce's API.
   - **Perform requests at any time**: lets Barometer keep the connection without you signing in again each time.

   Click **Allow**. These are Salesforce's own names for the two OAuth scopes we request, `api` and `refresh_token` ([OAuth Tokens and Scopes](https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_tokens_scopes.htm&language=en_US&type=5)). Despite the wording, our service only ever sends reads.
5. You're returned to our page. It tells you whether the connection worked.

You need to finish within 10 minutes of clicking Continue. If you take longer, open the link again.

## Afterwards

### See the connection

1. In Setup, search for **OAuth Usage** in Quick Find, and open the External Client Apps OAuth Usage page.
2. Black Cloud Barometer is listed with the number of users connected. Click that number to see the user, and when the connection was first and last used.

Source: [Manage External Client App OAuth Usage](https://help.salesforce.com/s/articleView?id=xcloud.manage_external_client_app_usage.htm&language=en_US&type=5).

### Revoke the connection

On the same page, click **Revoke** next to the user you connected. The connection stops at once, and we'd need a new link and a new sign-in to connect again. Please tell your consultant, so that they don't start a run that will fail.

### Uninstall

1. Revoke the connection first, as above.
2. In Setup, search for **Installed Packages** in Quick Find, and open it.
3. Click **Uninstall** next to Black Cloud Barometer.
4. Choose whether to save a copy of the package's data. There's nothing to keep, because the package stores no data.
5. Select **Yes, I want to uninstall**, and click **Uninstall**.

Source: [Second-Generation Managed Packaging Developer Guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/pkg2_dev.pdf), "Uninstall a Second-Generation Managed Package".

Uninstalling removes everything in the package: our app and the install script. It doesn't remove:

- **Barometer Read Only.** The script created it as an ordinary permission set, so it isn't part of the package. Unassign it, then delete it.
- **The Barometer user, if you made one.** Deactivate it, and free the licence.

## Troubleshooting

| What you see | What it means | What to do |
| --- | --- | --- |
| Barometer Read Only isn't in Permission Sets after the install | The script found an existing permission set named or labelled Barometer Read Only, and left it alone | Check that set grants what's listed above and no more, or rename it and ask your consultant for help re-running the setup |
| The permission set's description says "Not granted: ..." | Your edition or org doesn't offer one of the permissions | Tell your consultant which ones. Some checks will be reported as not measurable |
| The install fails with "The post install script failed" | Our install script hit an error. It's written so this shouldn't happen | Send your consultant a screenshot. Nothing was changed in your org |
| The connection page says the link has expired, been used, or been withdrawn | Each link works once and lasts 72 hours | Ask your consultant for a new link |
| `API_DISABLED_FOR_ORG` | Either the user doesn't have API Enabled, because Barometer Read Only isn't assigned or is empty, or your org has no API access ([REST API Developer Guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/api_rest.pdf)) | Check step 4, and that the permission set includes API Enabled. On Professional Edition, ask Salesforce about the API add-on |
| Salesforce signs you in as yourself, when you meant to use the Barometer user | The browser was already signed in to Salesforce | Close the window, and open the link in a new private window |
| Salesforce says you aren't allowed to use the app | You set Permitted Users in step 5, and the user you signed in as doesn't hold Barometer Read Only | Sign in as the Barometer user, or undo step 5 |
| It took more than 10 minutes, and the page says to start again | The sign-in has to finish within 10 minutes | Open the link again. It still works if it hasn't been used |

---

## Notes for Black Cloud

Not for the customer. Remove this section before sending the guide.

**Placeholders to fill**

- The install link: needs a promoted version id. Also confirm whether that version has an installation key (`sf package version create` takes `--installation-key` or `--installation-key-bypass`), and how the key reaches the customer.
- The service's hosting region and provider (QUESTIONS.md 1, 2 and 5).
- Zero data retention with Anthropic (QUESTIONS.md 8). The guide shouldn't go out with that line until it's settled.
- The worker's outbound IP addresses, for customers whose profiles have login IP ranges.
- How the connection page asks for the customer's My Domain login address, if it does. `hc connect start` takes `--login-url`, and the console pages are Milestone 5.

**Checked against Salesforce's documentation on 4 November 2026**

- Install by URL, install options, the unauthorised package notice, uninstall steps, "all components in the package ... are deleted", and Apex classes in a managed package have IP protection: 2GP guide (API 68.0) PDF.
- API editions and `API_DISABLED_FOR_ORG`: REST API guide (API 68.0) PDF.
- Scope labels "Manage user data via APIs" (`api`) and "Perform requests at any time" (`refresh_token, offline_access`): help article "OAuth Tokens and Scopes". The `api` description also says it covers Bulk API 2.0.
- Steps for Permitted Users ("External Client Apps Manager" in Quick Find, Edit Policies, "Admin approved users are pre-authorized"): help article "Preauthorize User App Access Through External Client App Policies". It says "External Client Apps Manager", while another help article says "External Client App Manager". Typing "External Client App" in Quick Find finds either.
- Refresh token option "Expire refresh token if not used for specific time", in the Policies tab under App Authorization: help article "Limit the Idle Refresh Token TTL".
- OAuth Usage page, usage by user, Revoke and Bulk Revoke: help article "Manage External Client App OAuth Usage".
- IP relaxation behaviour: help article for **connected apps**, not external client apps. The external client app metadata has the same four values (`Enforce`, `Bypass`, `Bypass_2factor`, `Enforce_RelaxRefresh`), so it's probably the same, but that's an inference.

**Sign in and go (DECISIONS.md 151 and 152)**

- Any user of the org may now connect. Whether customers still need the package for a sign-in by any user (rather than only for the install script that creates Barometer Read Only) depends on how the External Client App reaches their org, and wasn't decided in that change. If the package is still needed, step 1 stays as it is.

**Not verified (also in QUESTIONS.md 28 to 33)**

- The exact control for choosing the permission set once Permitted Users is admin-approved. The help article only says to select "the profile or permission set that should have access". The metadata field is `commaSeparatedPermissionSet`, which is how `bc-hc-dev`'s JWT app is set.
- Whether Salesforce shows the approval page at all to a pre-authorised user, given we send `prompt=login consent`. The help article says pre-authorised users "bypass the Allow or Deny permission window", and the web server flow article says `consent` makes the server "prompt the user for reapproval". Step 6 says "may show" for this reason. Every live sign-in so far was with "All users may self-authorize".
- That uninstalling leaves the script-created permission set. It follows from the 2GP guide (only package components are deleted), but nobody has uninstalled the package yet.
- Whether uninstalling the package also revokes existing tokens. The guide tells the admin to revoke first, so it doesn't matter for them.
- How many Salesforce Integration licences each edition includes. Only secondary sources say five for Enterprise, Unlimited and Performance, so the guide says "several". The help article wouldn't load.
- Whether the Salesforce API Integration permission set licence is needed, given Barometer Read Only has no licence. Our test users had it, so the guide keeps it.
- Whether a new API-only user can follow Salesforce's welcome or password reset email in a browser. On `bc-hc-dev` the password was set with anonymous Apex, `System.setPassword`, which customers shouldn't be asked to do. Test this with the first install of a released version.
- "Download AppExchange Packages" as the permission needed to install comes only from search extracts of the AppExchange install guide, so the guide asks for a System Administrator instead.
- Whether "Install for Admins Only" is offered at all for a package with no custom objects. Older AppExchange install guide text says the security step is skipped in that case.
- **The package grants read on `Task.WhatId` and `Task.WhoId` only.** DECISIONS.md 59 says "Task and Event `WhoId` and `WhatId`". The script and the reference permission set agree with each other, and the runs on `bc-hc-dev` and `bc-hc-scratch` used the same Task-only grant, but nobody checked whether events were visible in either. So either the decision row is wrong, or Event needs its own grant. Worth checking before the first customer run.
- The permission labels in the table under "What the permission set grants" are Setup's usual labels, written from memory. The API names beside them come from the script and are exact.
