# Custom providers

Use `defineQueueProvider` from the public `custom` entrypoint. A custom provider implements the public `QueueProvider` contract, advertises accurate capabilities, and can preserve arbitrary native option and response types. It must not import QueueKit internals.

`createQueueKit({ providers })` builds a typed factory for a map of custom provider definitions. The in-memory provider at `@mohamedhabibwork/queuekit/testing` is useful for contract-oriented application tests.
