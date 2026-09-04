# Release process

This document describes the end-to-end release process for the
`@queue-kit/*` monorepo. Internal-facing — for the contributor overview,
see [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## TL;DR

```text
PRs with scoped Conventional Commits
        │
        ▼
release-please.yml
        │  (one PR per package, scoped to changelog)
        ▼
"chore(release@core): 0.1.0 → 0.2.0"
"chore(release@memory): 0.1.0 → 0.1.1"
        │
        │  squash-merge
        ▼
release-please pushes @queue-kit/<scope>-v<X.Y.Z> tag + creates GH release
        │
        ▼
publish.yml  →  quality gate  →  tag-vs-version  →  pnpm publish <pkg>
                                        └→  idempotent gh release guard
```

There is **no manual `pnpm publish`** — the only manual action is
merging the Release PR for each affected package.

## Components

| File                                       | Role                                                                                   |
| ------------------------------------------ | -------------------------------------------------------------------------------------- |
| `release-please-config.json`               | release-please config: per-package blocks + `node-workspace` plugin with peer-dep update |
| `.release-please-manifest.json`            | Current released version per package path. Bumped automatically by release-please.     |
| `.github/workflows/release-please.yml`     | Runs release-please on every push to `main`; manages the Release PRs and tag pushing.  |
| `.github/workflows/publish.yml`            | Tag-driven pipeline: detect-package → quality → check-version → pnpm publish → gh release guard. |
| `.github/workflows/ci.yml`                 | Whole-workspace build + runtime gate (Node 22 + 24, lint, typecheck, tests, publint).  |
| `scripts/release-check.sh`                 | Local pre-flight: tag-vs-package-version sync, working tree clean, on `main`, in sync with origin/main. |
| `scripts/release.sh`                       | Manual escape hatch for one-off version overrides (rare).                              |
| Per-package `CHANGELOG.md`                 | Generated and maintained by release-please from Conventional Commits.                  |

## Flow

### 1. Land changes

PRs are squash-merged to `main`. The PR title becomes the commit on `main`
and is what release-please reads to determine the next version bump.

- `feat(scope):` → minor bump for that package
- `fix(scope):` / `perf(scope):` / `refactor(scope):` → patch bump
- `feat(scope)!:` or `BREAKING CHANGE:` in body → major bump (after v1.0.0)
- `docs:` / `test:` / `build:` / `ci:` / `chore:` → no release

The `node-workspace` plugin with `update-peer-dependencies: true`
ensures that when `@queue-kit/core` ships a new version, the next
`memory` release bumps its `peerDependencies."@queue-kit/core": "^..."`
reference accordingly.

### 2. release-please opens one Release PR per package

On every push to `main`, `.github/workflows/release-please.yml` runs and:

- Inspects commits since each package's recorded version in
  `.release-please-manifest.json`.
- Determines the scope tag (`core`, `memory`) from files-changed +
  commit scope.
- Computes the next version from Conventional Commit types.
- Opens or updates a single PR titled
  `chore(release@<scope>): <new-version>` per package.
- That PR contains the version bump + the per-package `CHANGELOG.md`
  update + (via the plugin) any peer-dep updates across the workspace.

You can override a specific package's next version by editing
`.release-please-manifest.json` before merge.

### 3. Merge the Release PR(s)

Squash-merge each Release PR once everything looks right. release-please
detects the merge and:

- Pushes the `@queue-kit/<scope>-v<X.Y.Z>` tag.
- Creates a GitHub Release with auto-generated notes.

### 4. publish.yml runs (per tag)

The tag push fires `.github/workflows/publish.yml`. The `detect-package`
job parses the tag and exports three outputs:

```
package-name = "@queue-kit/core"     # or "memory"
package-dir   = "packages/core"      # or "packages/memory"
version       = "0.2.0"
```

The pipeline then:

1. **quality** — `pnpm install --frozen-lockfile`, lint, typecheck,
   `test:types`, full workspace `build`, publint on the matching
   package.
2. **check-version** — verifies the tag matches
   `packages/<dir>/package.json`'s `version`.
3. **publish** — `pnpm publish --access=public --provenance
   --no-git-checks` from the matching directory. `--no-git-checks`
   because the tag was just pushed by release-please and the local
   checkout has no matching commit yet.
4. **github-release** — idempotent guard: creates the GitHub Release
   only if release-please hasn't already done it.

### 5. CI keeps running

`.github/workflows/ci.yml` (Node 22 + 24) keeps validating every push to
`main` and every PR, independent of the release flow.

## Tag format reference

| Package           | Tag pattern                              | Example                          |
| ----------------- | ---------------------------------------- | -------------------------------- |
| `@queue-kit/core`   | `@queue-kit/core-v<X.Y.Z>`               | `@queue-kit/core-v0.2.0`         |
| `@queue-kit/memory` | `@queue-kit/memory-v<X.Y.Z>`             | `@queue-kit/memory-v0.1.1`       |

`publish.yml`'s trigger is `tags: ['@queue-kit/*-v*']` (a single pattern
covering all current and future packages).

## Manual escape hatch

Sometimes a release can't wait for release-please — hotfix on a deleted
branch, force-push situation, or release-please runner outage. The
`scripts/release.sh` script implements the manual flow for a single
package:

```bash
./scripts/release.sh core patch        # @queue-kit/core: 0.1.0 -> 0.1.1
./scripts/release.sh memory minor      # @queue-kit/memory: 0.1.0 -> 0.2.0
./scripts/release.sh core 1.4.2        # explicit version
```

Under the hood:

1. Verifies the working tree is clean and on `main`.
2. Calls `pnpm --filter @queue-kit/<pkg> version <bump>` (which updates
   that package's `package.json`, creates a commit, and tags it).
3. Pushes the commit + tag with `--follow-tags`.
4. publish.yml takes over and runs `pnpm publish` for that single
   package.

After any manual release, sync the manifest so release-please doesn't
open a "catch-up" Release PR on the next push:

```bash
node -e "
  const fs = require('fs');
  const m = JSON.parse(fs.readFileSync('.release-please-manifest.json'));
  m['packages/<pkg>'] = '<new-version>';
  fs.writeFileSync('.release-please-manifest.json', JSON.stringify(m, null, 2) + '\n');
"
git add .release-please-manifest.json
git commit -m "chore(release): sync manifest after manual release of @queue-kit/<pkg>"
```

## Required secrets

| Secret        | Where set                                  | Used by                     |
| ------------- | ------------------------------------------ | --------------------------- |
| `NPM_TOKEN`   | repo Settings → Secrets → Actions          | `publish.yml` pnpm publish  |

`GITHUB_TOKEN` (built-in) covers everything else (tag push, GitHub
Release creation, PR / issue writes).

## Troubleshooting

### "Tag does not match package.json"

`publish.yml` `check-version` failed. Most common causes:

- You bumped `packages/<pkg>/package.json` but didn't tag it (or vice versa).
- A previous manual `scripts/release.sh` forgot to update
  `.release-please-manifest.json`.

Fix: align `packages/<pkg>/package.json`'s `version` with the tag, or
force-push a corrected tag.

### "Release @queue-kit/<scope>-vX.Y.Z already exists"

That's the idempotent guard. `publish.yml`'s `github-release` step
checks for an existing release and skips creation. Safe to ignore.

### release-please didn't open a Release PR

That means there are no Conventional-Commit-eligible changes since the
last release (e.g. only `docs:` / `chore:` commits, or a change in an
unreleased package directory). That's correct behavior — no release
needed.

To force a release without code changes, edit
`.release-please-manifest.json` to the desired next version for that
package and commit with `chore(release): force <version> for <pkg>`.

### npm publish 403 / 401

- 401: `secrets.NPM_TOKEN` is missing or expired.
- 403: token lacks publish rights on the `@queue-kit` scope. Use a
  granular access token with `Read and write` for the `@queue-kit`
  scope at <https://www.npmjs.com/settings/~/tokens>.

### release-please failed: "GitHub Actions is not permitted to create or approve pull requests"

The repo setting that allows `GITHUB_TOKEN` to open PRs is OFF
(by default for new repos). Fix either way:

**(a) Easier — flip the repo setting (one-time):**

1. Repo → Settings → Actions → General
2. Under "Workflow permissions":
   - Select "Read and write permissions"
   - Check **"Allow GitHub Actions to create and approve pull requests"**
3. Save. No workflow or secret changes needed; release-please will
   pick this up on its next run.

**(b) Harder — use a PAT (no repo setting required):**

1. Create a classic PAT at <https://github.com/settings/tokens> with
   the `repo` scope (or a fine-grained token with `Contents: write` +
   `Pull requests: write` on this repo).
2. Add it as a repo secret named `RELEASE_PLEASE_TOKEN`.
3. The workflow picks it up via `secrets.RELEASE_PLEASE_TOKEN ||
   github.token` — no further changes.

The PAT path is preferable for unattended / multi-org setups; the
repo setting is fine for solo repos.

### Wrong package detected

`publish.yml` `detect-package` derives the package from the tag name.
If you accidentally push a tag like `@queue-kit/core-v0.2.0` for an
unreleased package, delete it locally AND remotely:

```bash
git tag -d @queue-kit/core-v0.2.0
git push origin :refs/tags/@queue-kit/core-v0.2.0
```
