# Changelog — @queue-kit/core

All notable changes to `@queue-kit/core` are documented here.
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

- **Provider contract** with strongly-typed native escape hatches
  (`native: { ... }` is typed per provider through a generic `ProviderTypes`
  registry — never `Record<string, unknown>`).
- **Capability discovery** instead of silent approximation
  (`queue.supports("priorities")`, `queue.assertCapability(...)` throws
  `UnsupportedCapabilityError`).
- **Portable worker engine**: poll → decode → validate → middleware → handler
  → ack / retry / dead-letter, with graceful shutdown and visibility
  extension built in. Reusable for community adapters.
- **`createQueue`, `defineJob`, `queue.worker`, `queue.schedule`** as the
  core ergonomic surface.
- **`queue.native()` / `queue.withNative(...)`** typed escape hatch
  exposing the underlying provider SDK object fully typed via
  `ProviderTypes["nativeClient"]`.
- **Portable retry policy** with strategy + backoff + fatal-error predicate
  (`when: (error) => isNetworkError(error)`).
- **Portable dead-lettering** via `deadLetter: { queue }` — queue-kit
  strategy, portable everywhere.
- **Typed job definitions** with `defineJob<TIn, TOut>(name, version?)`
  and Standard Schema validation (any Standard Schema library: zod, valibot, …).
- **Error taxonomy**: `QueueKitError` + 14 subclasses
  (`UnsupportedCapabilityError`, `RetryableError`, `FatalError`,
  `ValidationError`, …) plus `retryable()` / `fatal()` predicates.
- **`installGracefulShutdown`**: SIGTERM-aware drain across all queues
  owned by a process.
- **`FakeClock`**: deterministic time for tests — `clock.advance("5m")`
  makes all delayed messages visible without real sleeping.
- **`JsonSerializer`** + pluggable encoder/decoder hook for non-JSON
  payloads.
- **Testing utilities** under `@queue-kit/core/testing`:
  `captureJobs`, `waitForQueueEvent`, `waitForWorkerEvent`.
- **`createQueueRegistry`**: named-queue lookup so workers across processes
  share a single source of truth.
- **`describe` / `health` / `size` / `purge`** introspection surface.

### Design rules

- ESM-only, **zero runtime dependencies** — no SDK code shipped.
- Compiled under `strict`, `exactOptionalPropertyTypes`,
  `noUncheckedIndexedAccess`, `noImplicitOverride`, `verbatimModuleSyntax`,
  `isolatedModules`. No public `any`.
- Errors are never discarded: original errors are preserved on `error.cause`.
- Cross-runtime by construction: standard web APIs only
  (`AbortController`, `TextEncoder`, WebCrypto), no Node built-ins — runs
  on Node ≥ 20, Bun, Deno.

[0.1.0]: https://github.com/mohamedhabibwork/queuekit/releases/tag/%40queue-kit%2Fcore-v0.1.0
