#!/usr/bin/env bash
# scripts/release-check.sh
# -----------------------------------------------------------------------------
# Pre-flight checks before a release. Run from the monorepo root.
#
#   ./scripts/release-check.sh                  # checks all publishable packages
#   PKG=memory ./scripts/release-check.sh       # check a single package
#
# What it verifies:
#   1. Working tree is clean (no uncommitted changes).
#   2. We're on the `main` branch.
#   3. `origin/main` is reachable and we are up to date with it.
#   4. For each publishable package:
#      - `.release-please-manifest.json` agrees with `package.json` version.
#      - `package.json`'s "version" matches the latest matching `@queue-kit/<pkg>-vX.Y.Z` tag
#        (if release-please already shipped one).
#   5. Workspace quality gate: pnpm install + lint + typecheck + tests + build.
#
# Exit code is non-zero on the first failure with a clear message.
# Designed for: pre-commit hook, CI aggregator, or just `pnpm run release:check`.
# -----------------------------------------------------------------------------

set -euo pipefail

c_red='\033[0;31m'
c_grn='\033[0;32m'
c_yel='\033[0;33m'
c_off='\033[0m'

step() { printf "\n${c_yel}▶ %s${c_off}\n" "$*"; }
ok()   { printf "${c_grn}  ok${c_off}  %s\n" "$*"; }
die()  { printf "${c_red}  fail${c_off} %s\n" "$*" >&2; exit 1; }

PKG="${PKG:-}"   # optional single-package filter (memory | core | @queue-kit/memory | @queue-kit/core)

# 1. Clean tree
step "working tree clean"
git diff --quiet -- || die "uncommitted changes — commit or stash them first"
git diff --quiet --cached -- || die "staged-but-uncommitted changes — commit them first"
ok "no uncommitted changes"

# 2. On main
step "current branch is main"
current="$(git branch --show-current)"
[ "$current" = "main" ] || die "on '$current', switch to main: git checkout main"
ok "on main"

# 3. Up to date with origin
step "in sync with origin/main"
git fetch --quiet origin main
local_sha="$(git rev-parse HEAD)"
remote_sha="$(git rev-parse origin/main)"
[ "$local_sha" = "$remote_sha" ] || die "HEAD ($local_sha) ≠ origin/main ($remote_sha) — pull or rebase"
ok "HEAD == origin/main"

# Resolve target packages
publish_pkgs="$(node -e "
  const fs = require('fs');
  const cfg = JSON.parse(fs.readFileSync('release-please-config.json'));
  const target = process.env.PKG || '';
  for (const [path, def] of Object.entries(cfg.packages)) {
    const name = def['package-name'] || def.component;
    if (target && !name.endsWith('/' + target) && name !== target) continue;
    console.log(path + '|' + name);
  }
")"

[ -n "$publish_pkgs" ] || die "no publishable packages matched (PKG='$PKG')"

# 4. Per-package tag/version/manifest agreement
echo "$publish_pkgs" | while IFS='|' read -r dir pkg_name; do
  step "$pkg_name — version sync"
  pkg_version="$(node -p "require('./${dir}/package.json').version")"
  manifest_version="$(node -p "require('./.release-please-manifest.json')['${dir}']")"
  [ "$manifest_version" = "$pkg_version" ] \
    || die "${dir}/package.json ($pkg_version) ≠ .release-please-manifest.json ($manifest_version)"

  pkg_scope="${pkg_name#@queue-kit/}"
  latest_tag="$(git tag --list "@queue-kit/${pkg_scope}-v*" --sort=-version:refname | head -n1 || true)"
  if [ -n "$latest_tag" ]; then
    tag_version="${latest_tag#@queue-kit/${pkg_scope}-v}"
    [ "$pkg_version" = "$tag_version" ] \
      || die "${dir}/package.json ($pkg_version) ≠ $latest_tag ($tag_version)"
    ok "$pkg_name ($pkg_version) matches manifest and $latest_tag"
  else
    ok "$pkg_name ($pkg_version) matches manifest; no @queue-kit/${pkg_scope}-v* tag yet"
  fi
done

# 5. Workspace quality gate
step "workspace install"
pnpm install --frozen-lockfile
ok "pnpm install"

step "lint"
pnpm lint
ok "lint"

step "typecheck"
pnpm typecheck
ok "typecheck"

step "tests (unit + contract + type-level)"
pnpm test:types
ok "tests"

step "build (whole workspace)"
pnpm build
ok "build"

step "publint"
while IFS='|' read -r dir pkg_name; do
  pnpm dlx publint "$dir" --strict
done <<< "$publish_pkgs"
ok "publint"

printf "\n${c_grn}✓ All pre-release checks passed for: %s${c_off}\n" "$(echo "$publish_pkgs" | tr '\n' ' ')"
printf "  Next: merge the release-please Release PRs, or run scripts/release.sh <pkg> <bump> for a manual cut.\n"
