#!/usr/bin/env bash
# scripts/release.sh
# -----------------------------------------------------------------------------
# Manual release helper for a single package in the @mohamedhabibwork/* monorepo.
# The escape hatch for the release-please flow.
#
# Usage:
#   ./scripts/release.sh <pkg> patch                        # 0.1.0 -> 0.1.1
#   ./scripts/release.sh <pkg> minor                        # 0.1.0 -> 0.2.0
#   ./scripts/release.sh <pkg> major                        # 0.1.0 -> 1.0.0
#   ./scripts/release.sh <pkg> 1.4.2                        # explicit version
#
#   <pkg> = core | memory | @mohamedhabibwork/core | @mohamedhabibwork/memory
#
# After this script exits cleanly, .github/workflows/publish.yml picks up the
# @mohamedhabibwork/<scope>-v<X.Y.Z> tag and runs pnpm publish for that single package.
#
# IMPORTANT — after a manual release, sync the release-please manifest so it
# doesn't open a "catch-up" Release PR on the next push:
#
#   node -e "const fs=require('fs');const m=JSON.parse(fs.readFileSync('.release-please-manifest.json'));m['packages/<scope>']='<new-version>';fs.writeFileSync('.release-please-manifest.json',JSON.stringify(m,null,2)+'\n');"
#   git add .release-please-manifest.json
#   git commit -m "chore(release): sync manifest after manual release of <pkg>"
# -----------------------------------------------------------------------------

set -euo pipefail

c_red='\033[0;31m'
c_yel='\033[0;33m'
c_grn='\033[0;32m'
c_off='\033[0m'

step() { printf "\n${c_yel}▶ %s${c_off}\n" "$*"; }
ok()   { printf "${c_grn}  ok${c_off}  %s\n" "$*"; }
die()  { printf "${c_red}  fail${c_off} %s\n" "$*" >&2; exit 1; }

# --- guards -------------------------------------------------------------------
step "pre-flight"
[ "$(git branch --show-current)" = "main" ] || die "not on main — checkout main first"
git diff --quiet -- || die "uncommitted changes — commit or stash them"
git fetch --quiet origin main
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] \
  || die "HEAD ≠ origin/main — pull first"
ok "tree clean, on main, up to date with origin"

# --- args ---------------------------------------------------------------------
[ "$#" -ge 2 ] || die "usage: $0 <pkg> <patch|minor|major|X.Y.Z>"

raw_pkg="$1"
bump="$2"

# Normalise package name to scope
case "$raw_pkg" in
  core|@mohamedhabibwork/core)    scope="core";    pkg="@mohamedhabibwork/core"    ;;
  memory|@mohamedhabibwork/memory) scope="memory"; pkg="@mohamedhabibwork/memory"  ;;
  *) die "unknown package '$raw_pkg' — expected core | memory | @mohamedhabibwork/core | @mohamedhabibwork/memory" ;;
esac

dir="packages/$scope"
[ -f "$dir/package.json" ] || die "missing $dir/package.json"

# Resolve bump argument
case "$bump" in
  patch|minor|major) ;;
  *)
    if ! echo "$bump" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[a-z0-9.-]+)?$'; then
      die "invalid version: '$bump' — use patch|minor|major or a SemVer like 1.2.3"
    fi
    export npm_config_version="$bump"
    bump="--no-git-tag-version"
    ;;
esac

# --- bump ----------------------------------------------------------------------
old_version="$(node -p "require('./${dir}/package.json').version")"
step "bumping ${pkg}"
pnpm --filter "$pkg" version "$bump" -m "chore(release): %s for ${pkg}"
new_version="$(node -p "require('./${dir}/package.json').version")"
ok "$old_version -> $new_version"

# --- push ----------------------------------------------------------------------
step "pushing commit + tag"
git push --follow-tags
ok "pushed"

printf "\n${c_grn}✓ Released @mohamedhabibwork/%s v%s.${c_off}\n" "$scope" "$new_version"
printf "  publish.yml will run quality + check-version + pnpm publish.\n"
printf "  After publish, sync .release-please-manifest.json so release-please\n"
printf "  starts from the new version on its next run.\n"
