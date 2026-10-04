import type { ServiceBusMessage, ServiceBusReceivedMessage } from "@azure/service-bus";
import { BaseQueueProvider } from "../core/base-provider.js";
import { jsonCodec } from "../core/codec.js";
import { resolveDelay } from "../core/delay.js";
import { createConsumer } from "../core/lifecycle.js";
import { loadOptional } from "../core/load-optional.js";
import { QueueConfigError } from "../core/errors.js";
import type { AzureServiceBusConfig } from "../config.js";
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

type ServiceBusClient = import("@azure/service-bus").ServiceBusClient;
type ServiceBusSender = import("@azure/service-bus").ServiceBusSender;
type ServiceBusReceiver = import("@azure/service-bus").ServiceBusReceiver;

/** Native publish fields from the SDK's `ServiceBusMessage` (the envelope fills `body`). */
export type AzurePublishOptions = Omit<ServiceBusMessage, "body" | "correlationId" | "messageId">;
/** Native receive options (this type fills `ConsumerOptions.native`). */
export type AzureConsumerOptions = {
  /** @see ServiceBusReceiverOptions */
  readonly maxAutoLockRenewalDurationInMs?: number;
  /** How many messages to fetch per receive call. @default 10 */
  readonly batchSize?: number;
  /** Long-poll wait per receive call in ms. @default 1000 */
  readonly waitTimeMs?: number;
};
export type AzureAcknowledgement = QueueAcknowledgement<ServiceBusReceivedMessage>;

interface AzureEnvelope {
  readonly type?: string;
  readonly payload: unknown;
  readonly headers?: Record<string, unknown>;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly traceId?: string;
  readonly timestamp?: number;
}

export class AzureServiceBusProvider extends BaseQueueProvider<"azureservicebus"> {
  readonly name = "azureservicebus" as const;
  readonly capabilities: QueueCapabilities = {
    kind: "queue",
    publish: true,
    consume: true,
    batchPublish: true,
    delayed: true,
    retries: true,
    ttl: true,
    deadLetter: true,
    ack: true,
    nack: true,
    ordering: true,
    scheduling: true,
  };
  #sdk: typeof import("@azure/service-bus") | undefined;
  #client: ServiceBusClient | undefined;
  #senders = new Map<string, ServiceBusSender>();
  #receivers = new Set<ServiceBusReceiver>();
  constructor(readonly config: AzureServiceBusConfig) {
    super();
  }
  async #module(): Promise<typeof import("@azure/service-bus")> {
    return (this.#sdk ??= await loadOptional<typeof import("@azure/service-bus")>(
      "@azure/service-bus",
      this.name,
    ));
  }
  async #getClient(): Promise<ServiceBusClient> {
    if (this.#client) return this.#client;
    const { ServiceBusClient } = await this.#module();
    if (this.config.connectionString) {
      this.#client = new ServiceBusClient(this.config.connectionString);
    } else if (this.config.fullyQualifiedNamespace && this.config.credential) {
      this.#client = new ServiceBusClient(
        this.config.fullyQualifiedNamespace,
        this.config.credential as never,
      );
    } else {
      throw new QueueConfigError(
        "azureservicebus requires config.connectionString, or fullyQualifiedNamespace + credential.",
        { provider: this.name },
      );
    }
    return this.#client;
  }
  async #sender(destination: string): Promise<ServiceBusSender> {
    const name = destination || this.config.queue;
    if (!name) {
      throw new QueueConfigError(
        "azureservicebus publish requires a destination or config.queue.",
        { provider: this.name, operation: "publish" },
      );
    }
    let sender = this.#senders.get(name);
    if (!sender) {
      sender = (await this.#getClient()).createSender(name);
      this.#senders.set(name, sender);
    }
    return sender;
  }
  static #envelope<TPayload>(message: QueueMessage<TPayload>): AzureEnvelope {
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
  static #nativeMessage(
    envelope: AzureEnvelope,
    message: QueueMessage,
    options: PublishOptions<AzurePublishOptions> | undefined,
  ): ServiceBusMessage {
    const delay = resolveDelay(options);
    return {
      ...options?.native,
      body: envelope,
      applicationProperties: options?.native?.applicationProperties ?? (message.headers as never),
      messageId: options?.idempotencyKey ?? message.id,
      correlationId: message.correlationId,
      scheduledEnqueueTimeUtc:
        options?.native?.scheduledEnqueueTimeUtc ??
        (delay === undefined ? undefined : new Date(Date.now() + delay)),
    };
  }
  async publish<TPayload>(
    destination: string,
    message: QueueMessage<TPayload>,
    options?: PublishOptions<AzurePublishOptions>,
  ): Promise<PublishResult<"azureservicebus", void>> {
    const queue = destination || this.config.queue || "";
    let result: PublishResult<"azureservicebus", void> | undefined;
    await this.operation(
      {
        provider: this.name,
        operation: "publish",
        destination: queue,
        traceId: message.traceId,
        correlationId: message.correlationId,
        metadata: message.metadata,
      },
      async () => {
        await (
          await this.#sender(destination)
        ).sendMessages(
          AzureServiceBusProvider.#nativeMessage(
            AzureServiceBusProvider.#envelope(message),
            message,
            options,
          ),
        );
        result = { ok: true, provider: this.name, native: undefined };
      },
    );
    return result!;
  }
  async publishMany<TPayload>(
    destination: string,
    messages: readonly QueueMessage<TPayload>[],
    options?: PublishOptions<AzurePublishOptions>,
  ): Promise<readonly PublishResult<"azureservicebus", void>[]> {
    const queue = destination || this.config.queue || "";
    let result: readonly PublishResult<"azureservicebus", void>[] | undefined;
    await this.operation(
      {
        provider: this.name,
        operation: "publishMany",
        destination: queue,
        metadata: messages[0]?.metadata,
      },
      async () => {
        const nativeMessages = messages.map((message) =>
          AzureServiceBusProvider.#nativeMessage(
            AzureServiceBusProvider.#envelope(message),
            message,
            options,
          ),
        );
        await (await this.#sender(destination)).sendMessages(nativeMessages);
        result = nativeMessages.map(
          () => ({ ok: true, provider: this.name, native: undefined }) as const,
        );
      },
    );
    return result!;
  }
  async consume<TPayload>(
    destination: string,
    handler: QueueHandler<TPayload, ServiceBusReceivedMessage, AzureAcknowledgement>,
    options?: ConsumerOptions<AzureConsumerOptions>,
  ): Promise<QueueConsumer> {
    const queue = destination || this.config.queue;
    if (!queue) {
      throw new QueueConfigError(
        "azureservicebus consume requires a destination or config.queue.",
        { provider: this.name, operation: "consume" },
      );
    }
    const client = await this.#getClient();
    const receiver = client.createReceiver(queue, {
      receiveMode: this.config.receiveMode ?? "peekLock",
      maxAutoLockRenewalDurationInMs: options?.native?.maxAutoLockRenewalDurationInMs,
    });
    this.#receivers.add(receiver);
    const peekLock = (this.config.receiveMode ?? "peekLock") === "peekLock";
    const controller = new AbortController();
    let paused = false;
    const run = async () => {
      while (!controller.signal.aborted) {
        if (paused) {
          await new Promise((resolve) => setTimeout(resolve, 20));
          continue;
        }
        const received = await receiver.receiveMessages(options?.native?.batchSize ?? 10, {
          maxWaitTimeInMs: options?.native?.waitTimeMs ?? 1_000,
        });
        for (const raw of received) {
          if (controller.signal.aborted) return;
          let settled = false;
          const ack: AzureAcknowledgement = {
            native: raw,
            complete: async () => {
              if (!settled && peekLock) {
                await receiver.completeMessage(raw);
                settled = true;
              }
            },
            retry: async (retry) => {
              if (!settled && peekLock) {
                await receiver.abandonMessage(
                  raw,
                  retry?.native as
                    | { readonly [key: string]: number | boolean | string | Date | null }
                    | undefined,
                );
                settled = true;
              }
            },
            reject: async (reject) => {
              if (!settled && peekLock) {
                if (reject?.requeue === false) {
                  await receiver.deadLetterMessage(raw, {
                    deadLetterReason: "rejected",
                    deadLetterErrorDescription: "Message rejected by queuekit consumer.",
                  });
                } else {
                  await receiver.abandonMessage(raw);
                }
                settled = true;
              }
            },
          };
          let body = raw.body;
          if (typeof body === "string") body = jsonCodec.decode(body);
          const envelope = (body ?? {}) as AzureEnvelope;
          try {
            await handler({
              message: {
                id: raw.messageId === undefined ? undefined : String(raw.messageId),
                type: envelope.type,
                payload: envelope.payload as TPayload,
                headers: envelope.headers ?? {},
                attempt: raw.deliveryCount,
                timestamp: envelope.timestamp ?? raw.enqueuedTimeUtc?.getTime(),
                correlationId:
                  envelope.correlationId ??
                  (raw.correlationId === undefined ? undefined : String(raw.correlationId)),
                causationId: envelope.causationId,
                traceId: envelope.traceId,
                native: raw,
              },
              ack,
              signal: controller.signal,
            });
            if (options?.autoAck !== false) await ack.complete();
          } catch {
            if (options?.autoAck !== false && !settled && peekLock) {
              await receiver.abandonMessage(raw);
              settled = true;
            }
          }
        }
      }
    };
    void run();
    const consumer = createConsumer({
      pause: async () => {
        paused = true;
      },
      resume: async () => {
        paused = false;
      },
      close: async () => {
        controller.abort();
        this.#receivers.delete(receiver);
        await receiver.close();
      },
    });
    return consumer;
  }
  native(): ServiceBusClient | undefined {
    return this.#client;
  }
  async close(): Promise<void> {
    this.markClosed();
    await Promise.all([...this.#senders.values()].map((sender) => sender.close()));
    this.#senders.clear();
    await Promise.all(
      [...this.#receivers].map(async (receiver) => {
        try {
          await receiver.close();
        } catch {
          // receiver may already be closing from consumer.close()
        }
      }),
    );
    this.#receivers.clear();
    await this.#client?.close();
    this.#client = undefined;
  }
}

export async function createAzureServiceBus(
  config: AzureServiceBusConfig,
): Promise<AzureServiceBusProvider> {
  return new AzureServiceBusProvider(config);
}
