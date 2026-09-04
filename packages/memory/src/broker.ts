import { createId, type Clock, type QueueEnvelope, type QueueRef, type QueueSize } from "@mohamedhabibwork/core";

interface StoredMessage {
  messageId: string;
  seq: number;
  queuePhysical: string;
  envelope: QueueEnvelope;
  body: string;
  priority: number;
  /** Epoch ms when the message becomes visible again. */
  availableAt: number;
  /** Delivery attempt, sourced from the envelope (1 on first publish). */
  deliveryCount: number;
  /** Set while the message is checked out to a consumer. */
  receiptId: string | undefined;
  visibilityExpiresAt: number | undefined;
}

export interface MemorySendRequest {
  envelope: QueueEnvelope;
  body: string;
  delayMs?: number | undefined;
  priority?: number | undefined;
  idempotencyKey?: string | undefined;
}

export interface MemorySendOutcome {
  id: string;
  enqueuedAt: number;
  deduplicated: boolean;
}

/** A StoredMessage that has been checked out to a consumer. */
export type DeliveredMessage = StoredMessage & { receiptId: string };

export interface MemoryReceiveRequest {
  maxMessages: number;
  visibilityTimeoutMs: number;
  waitTimeMs: number;
  signal?: AbortSignal | undefined;
}

/**
 * The in-memory broker: per-queue message stores, delayed visibility,
 * priority ordering, visibility leases with expiry redelivery, and
 * idempotency-key deduplication. Not durable by design — it exists for
 * tests, examples and local development.
 */
export class MemoryBroker {
  private readonly queues = new Map<string, Map<string, StoredMessage>>();
  private readonly deduplication = new Map<string, string>();
  private readonly waiters = new Set<() => void>();
  private seq = 0;
  private disposed = false;

  constructor(
    private readonly clock: Clock,
    public readonly defaultVisibilityTimeoutMs: number,
  ) {
    // Wake blocked receivers whenever a FakeClock advances so delayed
    // messages are released deterministically in tests.
    const advance = (clock as { onAdvance?: unknown }).onAdvance;
    if (typeof advance === "function") {
      (advance as (listener: () => void) => () => void).call(clock, () => this.wake());
    }
  }

  send(ref: QueueRef, request: MemorySendRequest): MemorySendOutcome {
    this.assertAlive();
    const now = this.clock.now();

    if (request.idempotencyKey !== undefined) {
      const dedupId = `${ref.physical}\u0000${request.idempotencyKey}`;
      const existingMessageId = this.deduplication.get(dedupId);
      if (existingMessageId !== undefined) {
        return { id: existingMessageId, enqueuedAt: now, deduplicated: true };
      }
    }

    const messageId = createId();
    if (request.idempotencyKey !== undefined) {
      this.deduplication.set(`${ref.physical}\u0000${request.idempotencyKey}`, messageId);
    }
    const store = this.store(ref.physical);
    store.set(messageId, {
      messageId,
      seq: this.seq++,
      queuePhysical: ref.physical,
      envelope: request.envelope,
      body: request.body,
      priority: request.priority ?? 0,
      availableAt: now + Math.max(0, request.delayMs ?? 0),
      deliveryCount: request.envelope.attempt,
      receiptId: undefined,
      visibilityExpiresAt: undefined,
    });

    this.wake();
    return { id: messageId, enqueuedAt: now, deduplicated: false };
  }

  async receive(ref: QueueRef, request: MemoryReceiveRequest): Promise<DeliveredMessage[]> {
    this.assertAlive();
    const deadline = Date.now() + Math.max(0, request.waitTimeMs);

    for (;;) {
      if (request.signal?.aborted) {
        throw createAbortError();
      }
      this.sweepExpired();

      const delivered = this.checkout(ref, request);
      if (delivered.length > 0 || Date.now() >= deadline) {
        return delivered;
      }

      await Promise.race([this.waitForWake(), realSleep(25)]);
    }
  }

  /** Check out up to `maxMessages` visible messages. */
  private checkout(ref: QueueRef, request: MemoryReceiveRequest): DeliveredMessage[] {
    const now = this.clock.now();
    const visible = [...this.store(ref.physical).values()]
      .filter((m) => m.receiptId === undefined && m.availableAt <= now)
      .sort((a, b) => (b.priority !== a.priority ? b.priority - a.priority : a.seq - b.seq))
      .slice(0, Math.max(1, request.maxMessages));

    for (const message of visible) {
      message.receiptId = createId();
      message.visibilityExpiresAt = now + request.visibilityTimeoutMs;
    }
    return visible as DeliveredMessage[];
  }

  /** Return expired in-flight messages to the pending pool (delivery retry). */
  private sweepExpired(): void {
    const now = this.clock.now();
    let swept = false;
    for (const store of this.queues.values()) {
      for (const message of store.values()) {
        if (message.receiptId !== undefined && message.visibilityExpiresAt !== undefined && message.visibilityExpiresAt <= now) {
          message.receiptId = undefined;
          message.visibilityExpiresAt = undefined;
          message.availableAt = now;
          swept = true;
        }
      }
    }
    if (swept) this.wake();
  }

  ack(receipt: { queuePhysical: string; receiptId: string }): boolean {
    this.assertAlive();
    const store = this.queues.get(receipt.queuePhysical);
    if (store === undefined) return false;
    for (const [messageId, message] of store) {
      if (message.receiptId === receipt.receiptId) {
        store.delete(messageId);
        return true;
      }
    }
    return false;
  }

  nack(
    receipt: { queuePhysical: string; receiptId: string },
    options: { requeue: boolean; delayMs: number },
  ): boolean {
    this.assertAlive();
    const store = this.queues.get(receipt.queuePhysical);
    if (store === undefined) return false;
    for (const message of store.values()) {
      if (message.receiptId !== receipt.receiptId) continue;
      if (options.requeue) {
        message.receiptId = undefined;
        message.visibilityExpiresAt = undefined;
        message.availableAt = this.clock.now() + Math.max(0, options.delayMs);
      } else {
        store.delete(message.messageId);
      }
      this.wake();
      return true;
    }
    return false;
  }

  extendVisibility(receipt: { queuePhysical: string; receiptId: string }, extraMs: number): boolean {
    const store = this.queues.get(receipt.queuePhysical);
    if (store === undefined) return false;
    for (const message of store.values()) {
      if (message.receiptId === receipt.receiptId) {
        message.visibilityExpiresAt = this.clock.now() + Math.max(0, extraMs);
        return true;
      }
    }
    return false;
  }

  purge(ref: QueueRef): void {
    this.store(ref.physical).clear();
    for (const key of [...this.deduplication.keys()]) {
      if (key.startsWith(`${ref.physical}\u0000`)) this.deduplication.delete(key);
    }
    this.wake();
  }

  size(ref: QueueRef): QueueSize {
    const now = this.clock.now();
    const messages = [...this.store(ref.physical).values()];
    return {
      total: messages.length,
      pending: messages.filter((m) => m.receiptId === undefined && m.availableAt <= now).length,
      delayed: messages.filter((m) => m.receiptId === undefined && m.availableAt > now).length,
      inflight: messages.filter((m) => m.receiptId !== undefined).length,
    };
  }

  envelopeIds(queuePhysical: string): string[] {
    return [...this.store(queuePhysical).values()].map((m) => m.envelope.id);
  }

  snapshot(queuePhysical: string): Array<{
    messageId: string;
    attempt: number;
    state: "pending" | "inflight";
    availableAt: number;
    priority: number;
  }> {
    return [...this.store(queuePhysical).values()].map((m) => ({
      messageId: m.messageId,
      attempt: m.deliveryCount,
      state: m.receiptId === undefined ? "pending" : "inflight",
      availableAt: m.availableAt,
      priority: m.priority,
    }));
  }

  dispose(): void {
    this.disposed = true;
    this.queues.clear();
    this.deduplication.clear();
    this.wake();
  }

  private store(physical: string): Map<string, StoredMessage> {
    let store = this.queues.get(physical);
    if (store === undefined) {
      store = new Map();
      this.queues.set(physical, store);
    }
    return store;
  }

  private waitForWake(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.waiters.add(resolve);
    });
  }

  private wake(): void {
    const current = [...this.waiters];
    this.waiters.clear();
    for (const waiter of current) {
      waiter();
    }
  }

  private assertAlive(): void {
    if (this.disposed) {
      throw new Error("MemoryBroker has been disposed; create a new memory() provider");
    }
  }
}

function realSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createAbortError(): Error {
  const error = new Error("Receive aborted");
  error.name = "AbortError";
  return error;
}
