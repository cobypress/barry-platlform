# Testing Barometer from a second org

Until now, every sign-in has been to BlackCloudProd, where the Barometer app lives. A customer's org is different: Salesforce needs the app installed there first (decision 189). This test proves the whole path from an org that isn't ours, and lets the facts step (decisions 287 to 295) check the Salesforce names we assumed.

It takes about 30 minutes, plus up to an hour for the scan to finish. It's Black Cloud's own throwaway org, so no customer org is involved.

## 1. Make a Developer Edition org (5 minutes)

1. Go to developer.salesforce.com/signup.
2. Use your own name and coby@black-cloud.com for the email. The username must look like an email but be unique across Salesforce, for example `coby.barometer-test@black-cloud.com`. It doesn't receive mail.
3. Open the verification email, set a password, and sign in once. Note the username.

## 2. The free scan, as a prospect would see it (15 minutes)

1. In a private browser window, go to black-cloud.com/barometer-scan.
2. Ask for a scan:
   - **Email:** use `coby+detest@black-cloud.com`. A repeat request from an address that already has an open request is quietly ignored, and your earlier test may still be open.
   - **Company:** something like "DE Test".
3. In Slack, approve it.
   - The Gmail sender isn't set up yet, so the message says the email didn't go.
   - Open the console's Scans page and copy the connect link.
4. Open the link in the private window. You should see the new **"First time? Install our app"** box above the sign-in button.
   - Click **Install Black Cloud Barometer**.
   - Sign in as the Developer Edition user.
   - Choose **Install for Admins Only**, and tick that you acknowledge the app hasn't had a security review.
   - Wait for the confirmation email or the "Installation complete" page.
5. Go back to the connect page and click **Sign in with Salesforce**. Sign in as the same user, and allow access.
6. The page should say "Connected" with a link to your results. The scan usually takes under an hour.

**What to tell me:**

- Anything that looked wrong or confusing, with a screenshot if you can.
- Whether Salesforce ever showed an error, and its code if it did.
- How long the install took.
- Your score, and whether the results page and the grade badge look right.

## 3. A full check on the same org (optional, 10 minutes plus the run)

The scan makes no documents, so its facts stay on the worker's disk. A full check writes them into the raw notes, under "Assumptions".

1. In the console, choose **New run**. Fill in the intake, leaving CPQ, Account Engagement and Service Cloud unticked.
2. On the run, make a connect link and open it. The app is already installed, so just sign in.
3. When the run finishes, generate the documents and open the raw notes. Look for the sentence "We checked N things about how this org is built…". Tell me N, and any fact listed as different.

A Developer Edition org has very little data, so many record checks will say "not measurable". That's expected. What matters here is that the connection, the reads and the documents all work from an org that isn't ours.

## Afterwards

Disconnect in the Developer Edition org under Setup, Connected Apps OAuth Usage. You can also uninstall the package from Installed Packages. Developer Edition orgs expire on their own if left unused.
