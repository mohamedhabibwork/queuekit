# Changelog — @queue-kit/memory

All notable changes to `@queue-kit/memory` are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes are generated automatically by
[`release-please`](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/) on merged PRs.
See [`CONTRIBUTING.md`](../../CONTRIBUTING.md) for the commit format and
[`.github/RELEASE.md`](../../.github/RELEASE.md) for the release process.

<!-- markdownlint-disable MD024 -->
## [0.1.0] - 2026-09-04

### Features

- **In-memory provider** for tests, examples, and local development.
  Not durable — never use in production.
- **Deterministic time** via `FakeClock` injection: delays, retries,
  and visibility leases only move when you call `clock.advance("...")`,
  making tests fully reproducible without real sleeps.
- **Visibility-timeout redelivery**: messages whose visibility lease
  expires before ack are redelivered automatically (covered by the
  `redelivers messages whose visibility lease expired before ack` test).
- **Auto-extension of visibility leases** for long-running handlers.
- **Ack / nack / dead-letter** semantics matching the portable
  `QueueKitError` taxonomy.
- **Priorities** via `native: { priority }` — higher priority first.
- **Idempotency-key deduplication** surfaced through `SendResult.deduplication`.
- **Typed test inspector** via `queue.native()` for assertions against
  the in-memory queue state.
- **`health` / `size` / `purge`** introspection.

### Design rules

- Cross-runtime (Node ≥ 20, Bun, Deno).
- Same `defineQueueProvider` contract as any future adapter — serves
  as the reference implementation the cross-provider contract tests
  are written against.
- Failures from handler / validation paths preserve `error.cause`.

### Dependency

- `peerDependencies."@queue-kit/core": "workspace:^"` (resolved to
  `^0.1.0` at publish).

[0.1.0]: https://github.com/mohamedhabibwork/queuekit/releases/tag/%40queue-kit%2Fmemory-v0.1.0
