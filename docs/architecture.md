# Architecture

QueueKit has a small runtime-neutral core and isolated provider adapters. The core owns message envelopes, codecs, middleware, consumers, lifecycle, errors, provider capabilities, registries, and the lazy manager. It never imports a broker SDK.

Every adapter preserves native configuration, responses, incoming messages, and acknowledgements through generic types. A provider advertises only capabilities that have real semantics for that broker. QueueKit does not translate Kafka partitions into priority, or a BullMQ attempt count into an SQS delivery guarantee.

`createQueueManager` constructs providers lazily. Calling `manager.provider('events')` is the point at which that provider may load its SDK; `warmup()` explicitly initializes all configured providers. `close()` is idempotent and closes initialized consumers and clients.
