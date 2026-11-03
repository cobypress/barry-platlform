#!/bin/bash
# Deploys the development-only JWT app (salesforce/dev-only) to bc-hc-dev, so
# the exit test can run as the read-only integration user without a browser.
# The certificate stays out of the repo: it's injected into a temporary copy.
# Afterwards the app's consumer key is saved to ~/.barometer/dev (mode 600).
# Nothing is printed that could hold a key or token.
set -euo pipefail

ALIAS="bc-hc-dev"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
DEV="$HOME/.barometer/dev"
CERT="$DEV/bc-hc-dev-jwt.crt"
CONFIG="$DEV/bc-hc-dev-jwt.json"
USERNAME="${HC_DEV_JWT_USERNAME:-barometer-read-only@black-cloud.com.bc-health-check}"

type=$(sf data query --target-org "$ALIAS" --query "SELECT OrganizationType FROM Organization" --json 2>/dev/null | jq -r '.result.records[0].OrganizationType')
if [ "$type" != "Developer Edition" ]; then
  echo "Stopped: $ALIAS is '$type', not a Developer Edition org." >&2
  exit 1
fi
[ -f "$CERT" ] || { echo "Stopped: no certificate at $CERT" >&2; exit 1; }

umask 077
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cp -R "$HERE/salesforce/." "$TMP/"
rm -rf "$TMP/.sf" "$TMP/.sfdx"
body=$(grep -v -- "-----" "$CERT" | tr -d '\n')
sed -i '' "s|DEV_JWT_CERTIFICATE|$body|" "$TMP/dev-only/main/default/extlClntAppGlobalOauthSets/Barometer_Dev_Jwt.ecaGlblOauth-meta.xml"

(cd "$TMP" && sf project deploy start --source-dir dev-only --target-org "$ALIAS" --json 2>/dev/null) | jq '{status, message, success: .result.success}'

(cd "$TMP" && sf project retrieve start --metadata "ExtlClntAppGlobalOauthSettings:Barometer_Dev_Jwt" --target-metadata-dir "$TMP/out" --unzip --target-org "$ALIAS" --json >/dev/null 2>&1)
key=$(grep -o '<consumerKey>[^<]*' "$TMP/out/unpackaged/unpackaged/extlClntAppGlobalOauthSets/Barometer_Dev_Jwt.ecaGlblOauth" | sed 's/<consumerKey>//')
[ -n "$key" ] || { echo "Stopped: the retrieve didn't return a consumer key" >&2; exit 1; }
instance=$(sf org display --target-org "$ALIAS" --json 2>/dev/null | jq -r '.result.instanceUrl')
jq -n --arg clientId "$key" --arg username "$USERNAME" --arg instanceUrl "$instance" --arg privateKeyPath "$DEV/bc-hc-dev-jwt.key" \
  '{clientId: $clientId, username: $username, instanceUrl: $instanceUrl, audience: "https://login.salesforce.com", privateKeyPath: $privateKeyPath}' > "$CONFIG"
chmod 600 "$CONFIG"
echo "Saved the dev JWT settings to $CONFIG"
