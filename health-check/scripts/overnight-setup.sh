#!/bin/bash
# Prepares the overnight worktrees described in docs/OVERNIGHT.md. Run it once.
# It commits Milestone 1 locally, because a worktree only sees commits. It
# pushes nothing, and it leaves .DS_Store and the plan document uncommitted.
set -euo pipefail

ROOT="${HC_ROOT:-/Users/cobypress/Desktop/Black Cloud}"
BARRY="$ROOT/barry-platlform"
NEXT="$ROOT/blackcloud-next"
TREES="$ROOT/worktrees"

for repo in "$BARRY" "$NEXT"; do
  branch=$(git -C "$repo" branch --show-current)
  if [ "$branch" != "health-check" ]; then
    echo "Stopped: $repo is on '$branch', not health-check." >&2
    exit 1
  fi
done

if [ -n "$(git -C "$BARRY" status --porcelain -- health-check)" ]; then
  git -C "$BARRY" add -- health-check
  git -C "$BARRY" commit -q -m "new - health check - milestone 1" -- health-check
  echo "Committed Milestone 1 on health-check (local only)."
fi

mkdir -p "$TREES"
git -C "$BARRY" worktree add -q -b hc/domains "$TREES/hc-domains" health-check
git -C "$BARRY" worktree add -q -b hc/reports "$TREES/hc-reports" health-check
git -C "$BARRY" worktree add -q -b hc/integration "$TREES/hc-integration" health-check
git -C "$NEXT" worktree add -q -b hc/console "$TREES/hc-console" health-check

# Lets a worker run unattended, and keeps it away from anything live. A prompt
# for a command outside this list pauses that worker until morning.
write_settings() {
  mkdir -p "$1/.claude"
  cat > "$1/.claude/settings.local.json" <<'JSON'
{
  "permissions": {
    "defaultMode": "acceptEdits",
    "allow": [
      "Bash(npm:*)", "Bash(npx:*)", "Bash(node:*)",
      "Bash(git status:*)", "Bash(git diff:*)", "Bash(git log:*)", "Bash(git show:*)",
      "Bash(git add:*)", "Bash(git commit:*)", "Bash(git merge:*)", "Bash(git branch:*)",
      "Bash(ls:*)", "Bash(mkdir:*)", "Bash(cp:*)", "Bash(mv:*)", "Bash(grep:*)", "Bash(find:*)", "Bash(wc:*)", "Bash(sort:*)", "Bash(python3:*)",
      "WebSearch", "WebFetch"
    ],
    "deny": [
      "Bash(sf:*)", "Bash(sfdx:*)", "Bash(git push:*)", "Bash(vercel:*)", "Bash(npx vercel:*)", "Bash(npx prisma migrate:*)", "Bash(npx prisma db:*)", "Bash(psql:*)", "Bash(redis-cli:*)",
      "Read(**/.env*)", "Read(~/.sfdx/**)", "Read(~/.sf/**)", "Read(//Users/cobypress/Desktop/Clients Orgs/**)"
    ]
  }
}
JSON
}

for tree in hc-domains hc-reports hc-integration; do
  write_settings "$TREES/$tree/health-check"
  (cd "$TREES/$tree/health-check" && npm ci --silent)
done
write_settings "$TREES/hc-console"
(cd "$TREES/hc-console" && npm ci --silent)

# The console tree is a different repo, so give it the plan and the schemas.
mkdir -p "$TREES/hc-console/docs/health-check"
cp "$BARRY/health-check/docs/OVERNIGHT.md" "$TREES/hc-console/docs/health-check/"
cp -R "$BARRY/health-check/src/schemas" "$TREES/hc-console/docs/health-check/engine-schemas"
rm -rf "$TREES/hc-console/docs/health-check/engine-schemas/__tests__"

for repo in "$BARRY" "$NEXT"; do
  exclude="$repo/$(git -C "$repo" rev-parse --git-path info/exclude)"
  grep -qx '.claude/settings.local.json' "$exclude" 2>/dev/null || echo '.claude/settings.local.json' >> "$exclude"
done

echo "Ready. Worktrees are in $TREES"
