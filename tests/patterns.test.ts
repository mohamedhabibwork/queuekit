import { describe, expect, it, vi } from "vitest";
import {
  QueueTimeoutError,
  computeBackoff,
  createMemoryIdempotencyStore,
  gracefulShutdown,
  scheduleRecurring,
  withIdempotency,
  withRetry,
  withTimeout,
} from "../src/index.js";
import { createMemoryQueue } from "../src/testing.js";

describe("computeBackoff", () => {
  it("returns fixed and capped exponential delays", () => {
    expect(computeBackoff({ backoff: { type: "fixed", delay: 50 } }, 3)).toBe(50);
    const policy = { backoff: { type: "exponential", delay: 100, maxDelay: 300 } } as const;
    expect(computeBackoff(policy, 1)).toBe(100);
    expect(computeBackoff(policy, 2)).toBe(200);
    expect(computeBackoff(policy, 3)).toBe(300);
    expect(computeBackoff({}, 2)).toBe(0);
  });
});

describe("withRetry", () => {
  it("retries with backoff then dead-letters when attempts are exhausted", async () => {
    const queue = createMemoryQueue();
    queue.setMaxAttempts(100);
    let calls = 0;
    await queue.consume(
      "jobs",
      withRetry(
        () => {
          calls++;
          throw new Error("boom");
        },
        { attempts: 3, backoff: { type: "fixed", delay: 0 } },
      ),
    );
    await queue.publish("jobs", { payload: 1 });
    await queue.flush();
    await queue.flush();
    expect(calls).toBe(3);
    expect(queue.deadLetters("jobs")).toHaveLength(1);
  });

  it("calls onFailure and succeeds on a later attempt", async () => {
    const queue = createMemoryQueue();
    const failures: number[] = [];
    let calls = 0;
    await queue.consume(
      "jobs",
      withRetry(
        () => {
          if (++calls < 2) throw new Error("flaky");
        },
        { attempts: 3, onFailure: (_e, attempt) => void failures.push(attempt) },
      ),
    );
    await queue.publish("jobs", { payload: 1 });
    await queue.flush();
    expect(failures).toEqual([1]);
    expect(queue.acknowledged("jobs")).toHaveLength(1);
  });
});

describe("withTimeout", () => {
  it("fails slow handlers with QueueTimeoutError and aborts the signal", async () => {
    let aborted = false;
    const handler = withTimeout(async ({ signal }) => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      aborted = signal.aborted;
    }, 5);
    const ack = { native: null, complete: vi.fn() };
    const message = { payload: 1, headers: {}, native: null };
    await expect(
      handler({ message, ack, signal: new AbortController().signal }),
    ).rejects.toBeInstanceOf(QueueTimeoutError);
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(aborted).toBe(true);
  });
});

describe("withIdempotency", () => {
  it("skips duplicate message ids and acknowledges them", async () => {
    const queue = createMemoryQueue();
    const seen: number[] = [];
    await queue.consume<number>(
      "orders",
      withIdempotency(({ message }) => {
        seen.push(message.payload);
      }),
    );
    await queue.publish("orders", { id: "o1", payload: 1 });
    await queue.publish("orders", { id: "o1", payload: 1 });
    await queue.publish("orders", { id: "o2", payload: 2 });
    await queue.waitUntilIdle();
    expect(seen).toEqual([1, 2]);
    expect(queue.acknowledged("orders")).toHaveLength(3);
  });

  it("expires keys after the store ttl", async () => {
    let now = 0;
    const store = createMemoryIdempotencyStore({ now: () => now });
    await store.set("k", 10);
    expect(await store.has("k")).toBe(true);
    now = 11;
    expect(await store.has("k")).toBe(false);
  });
});

describe("memory queue idempotencyKey", () => {
  it("deduplicates publishes that share an idempotency key", async () => {
    const queue = createMemoryQueue();
    const first = await queue.publish("x", { payload: 1 }, { idempotencyKey: "k" });
    const second = await queue.publish("x", { payload: 1 }, { idempotencyKey: "k" });
    expect(second.messageId).toBe(first.messageId);
    expect(queue.messages("x")).toHaveLength(1);
  });
});

describe("scheduleRecurring", () => {
  it("publishes on an interval until stopped", async () => {
    vi.useFakeTimers();
    try {
      const queue = createMemoryQueue();
      const job = scheduleRecurring(queue, "reports", { payload: "daily" }, { every: 1000 });
      await vi.advanceTimersByTimeAsync(3500);
      job.stop();
      await vi.advanceTimersByTimeAsync(3000);
      expect(queue.messages("reports")).toHaveLength(3);
      expect(job.runs).toBe(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("supports immediate runs and reports errors", async () => {
    const queue = createMemoryQueue();
    const errors: unknown[] = [];
    queue.failNext(new Error("down"));
    const job = scheduleRecurring(
      queue,
      "r",
      { payload: 1 },
      { every: 60_000, immediate: true, onError: (e) => errors.push(e) },
    );
    await new Promise((resolve) => setTimeout(resolve, 5));
    job.stop();
    expect(errors).toHaveLength(1);
    expect(() => scheduleRecurring(queue, "r", { payload: 1 }, { every: 0 })).toThrow();
  });
});

describe("gracefulShutdown", () => {
  it("closes consumers before queues", async () => {
    const order: string[] = [];
    const consumer = { close: async () => void order.push("consumer") };
    const queue = { close: async () => void order.push("queue") };
    const shutdown = gracefulShutdown({ consumers: [consumer], queues: [queue], signals: [] });
    await shutdown.shutdown();
    await shutdown.shutdown();
    expect(order).toEqual(["consumer", "queue"]);
  });

  it("rejects with QueueTimeoutError when closing hangs", async () => {
    const shutdown = gracefulShutdown({
      consumers: [{ close: () => new Promise<void>(() => {}) }],
      signals: [],
      timeout: 5,
    });
    await expect(shutdown.shutdown()).rejects.toBeInstanceOf(QueueTimeoutError);
  });
});

describe("resolveDelay", () => {
  it("prefers delay, else derives it from scheduleAt", async () => {
    const { resolveDelay } = await import("../src/core/delay.js");
    expect(resolveDelay({ delay: 5, scheduleAt: new Date(100) }, 0)).toBe(5);
    expect(resolveDelay({ scheduleAt: new Date(100) }, 40)).toBe(60);
    expect(resolveDelay({ scheduleAt: new Date(10) }, 40)).toBe(0);
    expect(resolveDelay(undefined)).toBeUndefined();
  });
});
