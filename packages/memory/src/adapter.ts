import {
  createCapabilities,
  type OutboundMessage,
  type QueueAdapter,
  type QueueRef,
  type ReceivedMessage,
  type AdapterSendOptions,
  type AdapterReceiveOptions,
  type AdapterNackOptions,
  type BatchSendResult,
  type HealthReport,
  type QueueSize,
  type SendResult,
} from "@queue-kit/core";
import { MemoryBroker, type DeliveredMessage } from "./broker";
import type { MemoryReceipt, MemorySendNativeResult, MemoryTypes } from "./types";

const memoryCapabilities = createCapabilities({
  send: { supported: true, mode: "native", details: { supportsBinary: true } },
  batchSend: { supported: true, mode: "emulated" },
  receive: { supported: true, mode: "native" },
  delayedDelivery: { supported: true, mode: "native" },
  priorities: { supported: true, mode: "native" },
  deduplication: { supported: true, mode: "emulated" },
  retries: { supported: true, mode: "emulated", details: { strategy: "queue-kit" } },
  deadLetterQueue: { supported: true, mode: "emulated", details: { strategy: "queue-kit" as const } },
  visibilityTimeout: { supported: true, mode: "native" },
  acknowledgements: { supported: true, mode: "native" },
  negativeAcknowledgements: { supported: true, mode: "native" },
  pauseResume: { supported: true, mode: "native" },
  concurrency: { supported: true, mode: "native" },
});

function toReceipt(message: { messageId: string; receiptId: string; queuePhysical: string }): MemoryReceipt {
  return {
    messageId: message.messageId,
    receiptId: message.receiptId,
    queuePhysical: message.queuePhysical,
  };
}

/**
 * Adapter over the in-memory broker. Implements the full Queue Kit data
 * path: send, batch, receive, ack, nack, visibility extension, purge, size,
 * health and the typed `native()` inspector.
 */
export class MemoryQueueAdapter implements QueueAdapter<MemoryTypes> {
  readonly id = "memory";
  readonly capabilities = memoryCapabilities;

  constructor(private readonly broker: MemoryBroker) {}

  async send(
    queue: QueueRef,
    message: OutboundMessage,
    options: AdapterSendOptions<MemoryTypes["send"]>,
  ): Promise<SendResult<string, MemorySendNativeResult>> {
    const outcome = this.broker.send(queue, {
      envelope: message.envelope,
      body: typeof message.body === "string" ? message.body : new TextDecoder().decode(message.body),
      delayMs: options.delayMs,
      priority: options.native?.priority,
      idempotencyKey: options.idempotencyKey,
    });
    return {
      id: outcome.id,
      envelopeId: message.envelope.id,
      queue: queue.name,
      provider: this.id,
      timestamp: outcome.enqueuedAt,
      deduplication: { mode: outcome.deduplicated ? "adapter" : "none" },
      native: { id: outcome.id, enqueuedAt: outcome.enqueuedAt, deduplicated: outcome.deduplicated },
    };
  }

  async sendBatch(
    queue: QueueRef,
    messages: readonly { message: OutboundMessage; options: AdapterSendOptions<MemoryTypes["send"]> }[],
  ): Promise<BatchSendResult<string, MemorySendNativeResult>> {
    const successful: SendResult<string, MemorySendNativeResult>[] = [];
    const failed: Array<{ index: number; error: unknown }> = [];
    for (const [index, entry] of messages.entries()) {
      try {
        successful.push(await this.send(queue, entry.message, entry.options));
      } catch (error) {
        failed.push({ index, error });
      }
    }
    return { successful, failed };
  }

  async receive<T>(
    queue: QueueRef,
    options: AdapterReceiveOptions<MemoryTypes["receive"]>,
  ): Promise<readonly ReceivedMessage<T, MemoryTypes>[]> {
    const stored = await this.broker.receive(queue, {
      maxMessages: options.maxMessages ?? 1,
      visibilityTimeoutMs: options.visibilityTimeoutMs ?? this.broker.defaultVisibilityTimeoutMs,
      waitTimeMs: options.waitTimeMs ?? 0,
      signal: options.signal,
    });
    return stored.map((message: DeliveredMessage) => ({
      id: message.envelope.id,
      name: message.envelope.name,
      body: message.body,
      payload: message.envelope.payload,
      attempt: message.deliveryCount,
      timestamp: message.envelope.timestamp,
      metadata: { ...(message.envelope.metadata ?? {}) },
      native: toReceipt(message),
      queue,
    })) as readonly ReceivedMessage<T, MemoryTypes>[];
  }

  async ack(message: ReceivedMessage<unknown, MemoryTypes>): Promise<void> {
    this.broker.ack(message.native);
  }

  async nack(
    message: ReceivedMessage<unknown, MemoryTypes>,
    options?: AdapterNackOptions<MemoryTypes["nack"]>,
  ): Promise<void> {
    this.broker.nack(message.native, {
      requeue: options?.requeue ?? true,
      delayMs: options?.delayMs ?? 0,
    });
  }

  async extendVisibility(message: ReceivedMessage<unknown, MemoryTypes>, extraMs: number): Promise<void> {
    this.broker.extendVisibility(message.native, extraMs);
  }

  async purge(queue: QueueRef): Promise<void> {
    this.broker.purge(queue);
  }

  async size(queue: QueueRef): Promise<QueueSize> {
    return this.broker.size(queue);
  }

  async health(): Promise<HealthReport> {
    return { status: "healthy", provider: this.id };
  }

  nativeClient(): MemoryTypes["nativeClient"] {
    return {
      size: async (queue: string) => this.broker.size({ name: queue, physical: queue }),
      envelopeIds: (queue: string) => this.broker.envelopeIds(queue),
      snapshot: (queue: string) => this.broker.snapshot(queue),
    };
  }

  async close(): Promise<void> {
    this.broker.dispose();
  }

  redactConfig(): Record<string, unknown> {
    return {};
  }
}
