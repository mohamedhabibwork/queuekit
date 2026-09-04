name: Pull request
description: Open a PR for the @mohamedhabibwork/* monorepo.
body:
  - type: markdown
    attributes:
      value: |
        Thanks for the PR! Fill out the template below — release-please
        reads the PR title to populate the changelog, so make sure the
        title follows the Conventional Commits spec. See CONTRIBUTING.md.

  - type: checkboxes
    id: checklist
    attributes:
      label: Pre-flight
      options:
        - label: I ran `pnpm install --frozen-lockfile && pnpm lint && pnpm typecheck && pnpm test:types && pnpm build` locally and it passes.
        - label: I added/updated tests for the change (or explained why none are needed in the PR description).
        - label: I read [CONTRIBUTING.md](./CONTRIBUTING.md) and the PR title follows Conventional Commits (`feat(core): ...`, `fix(memory): ...`, etc.).
        - label: I added CODEOWNERS coverage for any new files I touched.

  - type: dropdown
    id: package
    attributes:
      label: Affected package(s)
      description: Tick all that apply.
      multiple: true
      options:
        - "@mohamedhabibwork/core"
        - "@mohamedhabibwork/memory"
        - "examples"
        - "monorepo / tooling"

  - type: input
    id: scope
    attributes:
      label: Conventional-Commit scope tag
      description: |
        The `<scope>` segment of your PR title. release-please uses this to
        decide which package to release and to set up the Release PR.
        Use `core`, `memory`, or `workspace`. Omit only for repo-wide
        docs / chore changes.
      placeholder: "core"

  - type: dropdown
    id: release-impact
    attributes:
      label: Release impact
      options:
        - "feat (minor bump)"
        - "fix (patch bump)"
        - "perf (patch bump)"
        - "refactor (patch bump)"
        - "feat! / BREAKING CHANGE (major bump after v1.0.0)"
        - "none (docs / chore / ci / test)"

  - type: textarea
    id: what
    attributes:
      label: What
      description: 1-3 bullet summary of the change.
    validations:
      required: true

  - type: textarea
    id: why
    attributes:
      label: Why
      description: Link the issue / explain the trade-off.
    validations:
      required: true

  - type: textarea
    id: risk
    attributes:
      label: Risk / migration
      description: Any breaking changes, deprecations, or migration steps?

  - type: textarea
    id: testing
    attributes:
      label: Testing done
      description: Commands run + result. Paste CI run link if applicable.
