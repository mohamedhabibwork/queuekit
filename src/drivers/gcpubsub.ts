import type { Message as PubSubMessage } from "@google-cloud/pubsub";
import { BaseQueueProvider } from "../core/base-provider.js";
import { jsonCodec } from "../core/codec.js";
import { createConsumer } from "../core/lifecycle.js";
import { loadOptional } from "../core/load-optional.js";
import type { GooglePubSubConfig } from "../config.js";
import type {
  ConsumerOptions,
  PublishOptions,
  PublishResult,
  QueueAcknowledgement,
  QueueCapabilities,
  QueueConsumer,
  QueueHandler,
  QueueMessage,
} from "../core/types.js";

/** Native publish fields from the SDK's `MessageOptions` (minus the payload shortcuts). */
export type GcpPublishOptions = {
  readonly attributes?: Record<string, string>;
  readonly orderingKey?: string;
};
export type GcpConsumerOptions = {
  /** Passed to `pubsub.subscription(name, options)`. */
  readonly native?: { readonly batching?: unknown; readonly flowControl?: unknown };
};
export type GcpAcknowledgement = QueueAcknowledgement<PubSubMessage>;

interface GcpEnvelope {
  readonly type?: string;
  readonly payload: unknown;
  readonly headers?: Record<string, unknown>;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly traceId?: string;
  readonly timestamp?: number;
}

export class GooglePubSubProvider extends BaseQueueProvider<"gcpubsub"> {
  readonly name = "gcpubsub" as const;
  readonly capabilities: QueueCapabilities = {
    kind: "pubsub",
    publish: true,
    consume: true,
    batchPublish: true,
    delayed: false,
    retries: true,
    ttl: false,
    deadLetter: true,
    ack: true,
    nack: true,
    ordering: true,
    consumerGroups: true,
  };
  #sdk: typeof import("@google-cloud/pubsub") | undefined;
  #client: import("@google-cloud/pubsub").PubSub | undefined;
  #subscriptions = new Set<import("@google-cloud/pubsub").Subscription>();
  constructor(readonly config: GooglePubSubConfig) {
    super();
  }
  async #module(): Promise<typeof import("@google-cloud/pubsub")> {
    return (this.#sdk ??= await loadOptional<typeof import("@google-cloud/pubsub")>(
      "@google-cloud/pubsub",
      this.name,
    ));
  }
  async #getClient(): Promise<import("@google-cloud/pubsub").PubSub> {
    if (this.#client) return this.#client;
    const { PubSub } = await this.#module();
    this.#client = new PubSub({
      projectId: this.config.projectId,
      apiEndpoint: this.config.endpoint,
      credentials: this.config.credentials as never,
      keyFilename: this.config.keyFilename,
    });
    return this.#client;
  }
  static #envelope<TPayload>(message: QueueMessage<TPayload>): GcpEnvelope {
    return {
      type: message.type,
      payload: message.payload,
      headers: message.headers,
      correlationId: message.correlationId,
      causationId: message.causationId,
      traceId: message.traceId,
      timestamp: message.timestamp ?? Date.now(),
    };
  }
  async #topic() {
    return (await this.#getClient()).topic(this.config.topic);
  }
  async publish<TPayload>(
    destination: string,
    message: QueueMessage<TPayload>,
    options?: PublishOptions<GcpPublishOptions>,
  ): Promise<PublishResult<"gcpubsub", string>> {
    let result: PublishResult<"gcpubsub", string> | undefined;
    await this.operation(
      {
        provider: this.name,
        operation: "publish",
        destination: this.config.topic,
        traceId: message.traceId,
        correlationId: message.correlationId,
        metadata: message.metadata,
      },
      async () => {
        const messageId = await (
          await this.#topic()
        ).publishMessage({
          json: GooglePubSubProvider.#envelope(message),
          attributes: options?.native?.attributes,
          orderingKey: options?.native?.orderingKey,
        });
        result = { ok: true, provider: this.name, messageId, native: messageId };
      },
    );
    return result!;
  }
  async publishMany<TPayload>(
    destination: string,
    messages: readonly QueueMessage<TPayload>[],
    options?: PublishOptions<GcpPublishOptions>,
  ): Promise<readonly PublishResult<"gcpubsub", string>[]> {
    let result: readonly PublishResult<"gcpubsub", string>[] | undefined;
    await this.operation(
      {
        provider: this.name,
        operation: "publishMany",
        destination: this.config.topic,
        metadata: messages[0]?.metadata,
      },
      async () => {
        const topic = await this.#topic();
        result = await Promise.all(
          messages.map(async (message) => {
            const messageId = await topic.publishMessage({
              json: GooglePubSubProvider.#envelope(message),
              attributes: options?.native?.attributes,
              orderingKey: options?.native?.orderingKey,
            });
            return { ok: true, provider: this.name, messageId, native: messageId } as const;
          }),
        );
      },
    );
    return result!;
  }
  async consume<TPayload>(
    destination: string,
    handler: QueueHandler<TPayload, PubSubMessage, GcpAcknowledgement>,
    options?: ConsumerOptions<GcpConsumerOptions>,
  ): Promise<QueueConsumer> {
    const name = destination || this.config.subscription || this.config.topic;
    const client = await this.#getClient();
    const subscription = client.subscription(name, options?.native as never);
    this.#subscriptions.add(subscription);
    const consumerAbort = new AbortController();
    let paused = false;
    const onMessage = async (raw: PubSubMessage) => {
      if (paused) {
        // Pausing stops handler dispatch; nack keeps the message in the queue
        // (redelivered after the ack deadline) instead of growing the lease.
        raw.nack();
        return;
      }
      let envelope: GcpEnvelope;
      try {
        envelope = jsonCodec.decode(raw.data) as GcpEnvelope;
      } catch {
        raw.nack();
        return;
      }
      const ack: GcpAcknowledgement = {
        native: raw,
        complete: async () => raw.ack(),
        retry: async () => raw.nack(),
        // Dead-lettering on Pub/Sub is a subscription policy; nack (redeliver)
        // is the only client-side rejection.
        reject: async () => raw.nack(),
      };
      try {
        await handler({
          message: {
            id: raw.id,
            type: envelope.type,
            payload: envelope.payload as TPayload,
            headers: envelope.headers ?? {},
            attempt: raw.deliveryAttempt,
            timestamp: envelope.timestamp ?? Number(raw.publishTime),
            correlationId: envelope.correlationId,
            causationId: envelope.causationId,
            traceId: envelope.traceId,
            native: raw,
          },
          ack,
          signal: consumerAbort.signal,
        });
        if (options?.autoAck !== false) raw.ack();
      } catch {
        if (options?.autoAck !== false) raw.nack();
      }
    };
    subscription.on("message", (raw) => void onMessage(raw));
    const consumer = createConsumer({
      pause: async () => {
        paused = true;
      },
      resume: async () => {
        paused = false;
      },
      close: async () => {
        subscription.removeAllListeners("message");
        this.#subscriptions.delete(subscription);
        consumerAbort.abort();
      },
    });
    return consumer;
  }
  native(): import("@google-cloud/pubsub").PubSub | undefined {
    return this.#client;
  }
  async close(): Promise<void> {
    this.markClosed();
    for (const subscription of this.#subscriptions) {
      subscription.removeAllListeners("message");
      subscription.close();
    }
    this.#subscriptions.clear();
    await this.#client?.close();
    this.#client = undefined;
  }
}

export async function createGooglePubSub(
  config: GooglePubSubConfig,
): Promise<GooglePubSubProvider> {
  return new GooglePubSubProvider(config);
}
