# Architecture

Runtime-neutral queues, messaging, pub/sub, and streams. Every provider SDK is an optional peer
dependency loaded dynamically at creation time, so consumers install only what they use.

## Layers

Dependencies point one way, bottom-up:

| Layer                     | Contents                                                                                                                          | May depend on                        |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `src/core/`               | Base provider, codec, errors, lifecycle, `loadOptional`, types, handler wrappers (retry/timeout/idempotency), recurring, shutdown | itself only                          |
| `src/drivers/<provider>/` | One file per provider (azureservicebus, bullmq, cloudflare, gcpubsub, kafka, nats, rabbitmq, redis, sqs)                          | core, root `config.ts`, its own file |
| `src/testing/`            | In-memory reference implementation                                                                                                | core, config, itself — never drivers |
| `src/*.ts` (root)         | Composition: index, config, errors, factory, manager, registry, per-provider facades                                              | anything                             |

These rules are enforced by `tests/architecture.test.ts`: it walks every file under `src/`,
resolves each relative import, and fails when a layer reaches outside its boundary. If a
legitimate new edge is needed, widen the allow-list in the test and this document together.

### Adding a provider

1. Add the config shape to `src/config.ts` (the discriminated union drives everything).
2. Create `src/drivers/<name>.ts` extending `BaseQueueProvider`, loading the SDK via
   `loadOptional` so the package stays an optional peer dependency.
3. Add a facade `src/<name>.ts` and wire it into `src/factory.ts`.
4. `npm run check` — the architecture test proves the driver stayed self-contained.

## Clean-code toolchain

Linting and formatting are enforced by oxlint and oxfmt:

| Command                | What it does                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------- |
| `npm run lint`         | `oxlint --deny-warnings --report-unused-disable-directives` — fails on any warning |
| `npm run lint:fix`     | Auto-fix what oxlint can                                                           |
| `npm run format`       | `oxfmt` — canonical formatting for every source file                               |
| `npm run format:check` | CI gate for formatting                                                             |
| `npm run check`        | format:check → lint → build → test → type tests                                    |

The lint config (`.oxlintrc.json`) enables the `correctness`, `suspicious`, and `perf` categories
across the `typescript`, `unicorn`, `import`, and `promise` plugins, plus targeted rules:
`no-unused-vars` (with `_`-prefix opt-out), `no-console` (allowing `warn`/`error` only),
`prefer-node-protocol`, `no-array-reduce`, `no-require-imports`, `import/no-duplicates`,
`promise/no-nesting`, and `promise/always-return`. Tests and scripts get a narrower rule set via
`overrides`. Anything intentionally outside the rules is marked inline with a reason
(`// oxlint-disable-next-line <rule> -- why`) rather than a blanket exclusion.
