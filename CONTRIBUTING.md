# Contributing to Queue Kit

Thanks for your interest in `@mohamedhabibwork/*`! This guide covers how to set up
the project locally, the commit / PR conventions, and how releases are
produced across the monorepo.

## Project layout

```
queuekit/                       # this monorepo
├── packages/
│   ├── core/                   # @mohamedhabibwork/core    (publishable, queue primitives + worker engine)
│   └── memory/                 # @mohamedhabibwork/memory  (publishable, in-memory provider)
├── examples/                   # @mohamedhabibwork/examples (private, runnable demos)
├── .github/
│   ├── workflows/              # ci.yml, release-please.yml, publish.yml, ...
│   ├── RELEASE.md              # internal release-process doc
│   └── dependabot.yml          # version/security PRs
├── scripts/                    # release.sh, release-check.sh
├── release-please-config.json  # release-please config (per-package)
├── .release-please-manifest.json  # tracked released versions
├── pnpm-workspace.yaml
├── package.json                # root, PRIVATE: "queue-kit-monorepo"
└── vitest.config.ts            # vitest config (root)
```

## Packages & release flow

| Package | Published? | Versioned by | Tag format |
| --- | --- | --- | --- |
| `@mohamedhabibwork/core`   | yes | release-please | `@mohamedhabibwork/core-v<X.Y.Z>` |
| `@mohamedhabibwork/memory` | yes | release-please | `@mohamedhabibwork/memory-v<X.Y.Z>` |
| `@mohamedhabibwork/examples` | no (private) | manual | n/a |
| `queue-kit-monorepo` (root) | no (private) | manual | n/a |

Memory declares `@mohamedhabibwork/core` as a **peer-dependency** (`workspace:^`).
The `node-workspace` plugin in `release-please-config.json` (with
`update-peer-dependencies: true`) automatically bumps that peer-dep every
time a new `core` release is cut. Because the two packages are versioned
**independently**, a core bump does NOT force a memory release — memory
only ships its own version when its own commits warrant it.

See [`.github/RELEASE.md`](.github/RELEASE.md) for the full end-to-end flow.

## Prerequisites

- **Node.js ≥ 20** (engines is `>=20`; CI matrix is 22 + 24)
- **pnpm 11+** (the repo pins `packageManager: pnpm@11.15.1`; Corepack will use that automatically)
- A POSIX shell for running scripts

## Local setup

```bash
git clone git@github.com:mohamedhabibwork/queuekit.git
cd queuekit
pnpm install --frozen-lockfile
pnpm typecheck      # tsc --noEmit (whole workspace, exact-optional-property-types)
pnpm test:types     # vitest with --typecheck.enabled=true
pnpm build          # pnpm -r --workspace-concurrency=1 build
pnpm lint           # eslint packages examples
```

For one package:

```bash
pnpm --filter @mohamedhabibwork/core build
pnpm --filter @mohamedhabibwork/core test:types
```

## Commit message convention — Conventional Commits 1.0

The repo is **release-please-driven**: every release version (per
package) is derived automatically from the merged PR titles on `main`.

### Required format

```
<type>(<scope>): <short description>

<body — explain the why, not the what>

<footer — BREAKING CHANGE / Closes #issue / refs>
```

### Scope tag — the important bit

`<scope>` MUST be one of:

| Scope          | Targets package        | Bumps release-please track    |
| -------------- | ---------------------- | ----------------------------- |
| `core`         | `packages/core/**`     | `@mohamedhabibwork/core`             |
| `memory`       | `packages/memory/**`   | `@mohamedhabibwork/memory`           |
| `workspace`    | cross-cutting commits  | the touched packages         |
| (omitted)      | root, examples, docs   | none                          |

release-please uses PR files-changed (and commit message header) to
decide which package to release. Use the scope tag to make it
unambiguous.

### Allowed types and their version-bump impact

| Type             | Bump     | When to use                                    |
| ---------------- | -------- | ---------------------------------------------- |
| `feat`           | **minor** | A new user-visible feature or API addition    |
| `feat!`          | **major** | A breaking change (or use `BREAKING CHANGE:`) |
| `fix`            | **patch** | A bug fix                                      |
| `perf`           | **patch** | A performance improvement                     |
| `refactor`       | **patch** | Internal restructure with no behavior change  |
| `docs`           | none     | Docs-only changes                              |
| `test`           | none     | Test-only changes                              |
| `build`          | none     | Build / tooling changes                        |
| `ci`             | none     | CI / workflow changes                          |
| `chore`          | none     | Repo maintenance (deps, configs, ignores)      |
| `style`          | none     | Formatting only                                |
| `revert`         | none     | Reverts a previous commit                      |

### Breaking changes

A breaking change **must** be communicated two ways:

1. The commit / PR title uses a `!` after the type, e.g.
   `feat(core)!: drop legacy priority helpers`.
2. The commit body / PR description starts with
   `BREAKING CHANGE: <description>`.

Bumps become **major**. Until v1.0.0 we run `bump-minor-pre-major: true`
so a `feat!:` on `0.x.y` still bumps minor; once v1.0.0 is tagged,
breaking changes bump to v2.0.0.

### Examples

```text
feat(core): add rate-limit policy helper

Add `withRateLimit` for worker-level rate-limiting via a portable policy
`{ tokens, interval }`. Falls back to provider-native where supported
and throws `UnsupportedCapabilityError` otherwise.

Closes #42

---

fix(memory): release promise on FastClock.advance()

`clock.advance("5m")` resolved before all jobs had drained from the
in-memory queue. Now we await the queue's drain promise before
resolving the advance.

Closes #87
```

### PR titles == commit titles (after squash)

We use **squash-merge**, so the PR title becomes the commit on `main`.
Title your PR exactly as the commit would read; release-please uses that
title for changelog generation.

## Pull request process

1. **Branch off `main`.** Use a descriptive branch name:
   `feat/core-rate-limit`, `fix/memory-fast-clock-drain`, etc.
2. **Local checks before pushing:**
   ```bash
   pnpm install --frozen-lockfile
   pnpm lint
   pnpm typecheck
   pnpm test:types
   pnpm build
   ```
3. **Open a PR.** Fill out the PR template:
   - The "What" (1-3 bullet summary)
   - The "Why" (link the issue, explain the trade-off)
   - The "Risk" (any breaking-change / migration notes)
   - Testing done (commands + results)
4. **CI must pass.** The aggregator `verify` job (`.github/workflows/ci.yml`)
   runs lint + typecheck + tests + build + publint on Node 22 + 24.
5. **Squash-merge** once green. The PR title becomes the release commit.

## Adding a new provider

The monorepo is designed for community adapters via
`defineQueueProvider` in `@mohamedhabibwork/core`.

To add an in-repo provider package (`@mohamedhabibwork/<name>`):

1. Create `packages/<name>/` with the standard layout (src/, package.json,
   tsup config, README).
2. In `packages/<name>/package.json`, declare peer-deps:
   ```json
   "peerDependencies": { "@mohamedhabibwork/core": "workspace:^" }
   ```
3. Add an entry to `release-please-config.json` under `packages.<name>`
   (copy the `core` or `memory` block, change component / package-name).
4. Add the path to `pnpm-workspace.yaml` (`packages/*` already covers it).
5. Add CODEOWNERS coverage:
   ```
   /packages/<name>/  @mohamedhabibwork
   ```
6. Pass the cross-provider contract test scaffold in
   `packages/core/test/native-types.test-d.ts`.

## Documentation

The README is the canonical doc. Per-package READMEs are auto-discovered
on GitHub. No separate docs site yet (MkDocs considered but not adopted
because the README is the doc).

## Reporting security issues

Please **do not** open public GitHub issues for security reports. GitHub
private vulnerability disclosure should be enabled on this repo —
contact `mohamedhabibwork` to coordinate a fix.

## Release process

The short version:

1. Land PRs to `main` with Conventional Commit titles using a scope tag
   (`feat(core): ...`, `fix(memory): ...`).
2. `release-please` opens/updates one Release PR **per package** that
   has changes since the last release.
3. Review the Release PRs — squash-merge when ready.
4. `release-please` pushes the tag (`@mohamedhabibwork/<scope>-v<X.Y.Z>`) and
   creates the GitHub release.
5. `.github/workflows/publish.yml` picks up the tag → quality gate →
   publishes that single package to npm.

Manual escape hatch exists (`scripts/release.sh`) but the goal is to use
release-please exclusively.

Full details: [`.github/RELEASE.md`](.github/RELEASE.md).

## License

By contributing, you agree that your contributions will be licensed under
the [MIT License](./LICENSE).
