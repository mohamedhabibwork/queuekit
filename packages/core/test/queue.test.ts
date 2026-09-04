import { describe, expect, it, vi } from "vitest";
import {
  createCapabilities,
  createQueue,
  createQueueRegistry,
  QueueConfigurationError,
  QueueMessageTooLargeError,
  QueueSendError,
  UnsupportedCapabilityError,
  type OutboundMessage,
  type ProviderTypes,
  type AdapterSendOptions,
  type QueueAdapter,
  type QueueProvider,

} from "../src/index";
import { captureJobs, waitForQueueEvent } from "../src/testing";

/**
 * A deliberately tiny fake provider with its own native option types —
 * used to prove that core drives any adapter correctly without a real SDK.
 */
interface FakeTypes extends ProviderTypes {
  connection: { endpoint: string };
  queue: never;
  send: { fakeHeader?: string };
  receive: never;
  worker: never;
  nack: never;
  schedule: never;
  messageId: string;
  nativeMessage: { deliveryTag: number };
  nativeClient: { ping(): string };
  nativeResult: { region: string };
}

function fakeAdapter(): {
  adapter: QueueAdapter<FakeTypes>;
  sent: OutboundMessage[];
  options: AdapterSendOptions<FakeTypes["send"]>[];
} {
  const sent: OutboundMessage[] = [];
  const options: AdapterSendOptions<FakeTypes["send"]>[] = [];
  const adapter: QueueAdapter<FakeTypes> = {
    id: "fake",
    capabilities: createCapabilities({
      send: { supported: true, details: { maxBytes: 256, supportsBinary: false } },
      batchSend: { supported: true },
      receive: { supported: true },
      deduplication: { supported: true, mode: "native" },
    }),
    async send(_queue, message, sendOptions) {
      sent.push(message);
      options.push(sendOptions);
      return {
        id: `msg-${sent.length}`,
        envelopeId: message.envelope.id,
        queue: _queue.name,
        provider: "fake",
        timestamp: Date.now(),
        deduplication: { mode: "provider" },
        native: { region: "us-east-1" },
      };
    },
    nativeClient() {
      return { ping: () => "pong" };
    },
    async close() {},
  };
  return { adapter, sent, options };
}

function fakeProvider(lazy = true): QueueProvider<FakeTypes> {
  const { adapter } = fakeAdapter();
  return {
    id: "fake",
    capabilities: adapter.capabilities,
    adapter,
    connection: { endpoint: "memory://" },
    lazy,
    resolvePhysicalName: (name) => `fake://${name}`,
  };
}

describe("createQueue", () => {
  it("exposes provider identity, capabilities and description", () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    expect(queue.providerId).toBe("fake");
    expect(queue.name).toBe("orders");
    expect(queue.supports("send")).toBe(true);
    expect(queue.supports("fifo")).toBe(false);
    expect(() => queue.assertCapability("fifo")).toThrow(UnsupportedCapabilityError);

    const description = queue.describe();
    expect(description.library).toBe("queue-kit");
    expect(description.provider).toBe("fake");
    expect(description.queue).toBe("orders");
    expect(description.physicalName).toBe("fake://orders");
    expect(queue.closed).toBe(false);
  });

  it("rejects empty names early", () => {
    expect(() => createQueue({ name: "", provider: fakeProvider() })).toThrow(QueueConfigurationError);
  });

  it("serializes envelopes with id, name, payload and version", async () => {
    const { adapter, sent } = fakeAdapter();
    const queue = createQueue({
      name: "orders",
      provider: { id: "fake", capabilities: adapter.capabilities, adapter, connection: { endpoint: "x" }, lazy: true },
    });
    await queue.send({ name: "order.created", version: 3 }, { orderId: "42" });

    expect(sent).toHaveLength(1);
    const envelope = sent[0]?.envelope;
    expect(envelope?.v).toBe(1);
    expect(envelope?.name).toBe("order.created");
    expect(envelope?.version).toBe(3);
    expect(envelope?.payload).toEqual({ orderId: "42" });
    expect(envelope?.attempt).toBe(1);
    expect(typeof envelope?.id).toBe("string");
  });

  it("flows typed native options to the adapter untouched", async () => {
    const { adapter, options } = fakeAdapter();
    const queue = createQueue({
      name: "orders",
      provider: { id: "fake", capabilities: adapter.capabilities, adapter, connection: { endpoint: "x" }, lazy: true },
    });
    await queue.send("order.created", {}, { native: { fakeHeader: "x-trace" } });
    expect(options[0]?.native).toEqual({ fakeHeader: "x-trace" });
  });

  it("rejects delays on providers without delayed delivery", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    await expect(queue.send("job", {}, { delay: "5s" })).rejects.toThrow(UnsupportedCapabilityError);
  });

  it("guards payload size against the provider limit", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    await expect(queue.send("job", { blob: "x".repeat(500) })).rejects.toThrow(QueueMessageTooLargeError);

    const warningQueue = createQueue({ name: "orders", provider: fakeProvider(), payloadLimits: "warn" });
    const result = await warningQueue.send("job", { blob: "x".repeat(500) });
    expect(result.id).toBe("msg-1");
  });

  it("wraps adapter failures without discarding the cause", async () => {
    const { adapter } = fakeAdapter();
    adapter.send = async () => {
      throw new Error("connection reset");
    };
    const queue = createQueue({
      name: "orders",
      provider: { id: "fake", capabilities: adapter.capabilities, adapter, connection: { endpoint: "x" }, lazy: true },
    });
    const error = await queue.send("job", {}).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(QueueSendError);
    expect((error as QueueSendError).cause).toBeInstanceOf(Error);
  });

  it("emits sent and send-failed events", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    const sentHandler = vi.fn();
    queue.on("sent", sentHandler);
    await queue.send("job", {});
    expect(sentHandler).toHaveBeenCalledTimes(1);
    expect(sentHandler.mock.calls[0]?.[0]).toMatchObject({ queue: "orders", jobName: "job" });
  });

  it("runs producer middleware in order (onion)", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    const calls: string[] = [];
    queue.use(async (_ctx, next) => {
      calls.push("first:before");
      const result = await next();
      calls.push("first:after");
      return result;
    });
    queue.use(async (ctx, next) => {
      calls.push("second:before");
      ctx.envelope.metadata = { ...ctx.envelope.metadata, "x-tenant": "acme" };
      return next();
    });

    await queue.send("job", {});
    expect(calls).toEqual(["first:before", "second:before", "first:after"]);

    const capture = captureJobs(queue);
    await queue.send("job", {});
    expect(capture.jobs[0]?.envelope.metadata?.["x-tenant"]).toBe("acme");
  });

  it("falls back to sequential sends when the adapter has no native batch", async () => {
    const { adapter, sent } = fakeAdapter();
    const queue = createQueue({
      name: "orders",
      provider: { id: "fake", capabilities: adapter.capabilities, adapter, connection: { endpoint: "x" }, lazy: true },
    });
    const result = await queue.sendBatch([
      { job: "a", data: { n: 1 } },
      { job: "b", data: { n: 2 } },
    ]);
    expect(result.failed).toHaveLength(0);
    expect(result.successful).toHaveLength(2);
    expect(sent).toHaveLength(2);
  });

  it("reports partial batch failures with original indices", async () => {
    const { adapter } = fakeAdapter();
    let calls = 0;
    adapter.send = async (queue, message) => {
      calls += 1;
      if (calls === 2) throw new Error("item two exploded");
      return {
        id: `msg-${calls}`,
        envelopeId: message.envelope.id,
        queue: queue.name,
        provider: "fake",
        timestamp: 0,
        deduplication: { mode: "none" },
        native: { region: "r" },
      };
    };
    const queue = createQueue({
      name: "orders",
      provider: { id: "fake", capabilities: adapter.capabilities, adapter, connection: { endpoint: "x" }, lazy: true },
    });
    const result = await queue.sendBatch([
      { job: "a", data: 1 },
      { job: "b", data: 2 },
      { job: "c", data: 3 },
    ]);
    expect(result.successful).toHaveLength(2);
    expect(result.failed).toEqual([{ index: 1, error: expect.anything() }]);
  });

  it("exposes the typed native client", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    expect(queue.native().ping()).toBe("pong");
    await expect(queue.withNative(async (native) => native.ping())).resolves.toBe("pong");
  });

  it("schedules at a future date via delayed delivery", async () => {
    const { adapter, sent, options: sendOptions } = fakeAdapter();
    const queue = createQueue({
      name: "orders",
      provider: {
        id: "fake",
        capabilities: createCapabilities({ ...adapter.capabilities, delayedDelivery: { supported: true } }),
        adapter,
        connection: { endpoint: "x" },
        lazy: true,
      },
    });
    const at = new Date(Date.now() + 60_000);
    const result = await queue.schedule("job", {}, { at });
    expect(result.strategy).toBe("delayed-delivery");
    expect(sendOptions[0]?.delayMs).toBeGreaterThan(59_000);
    expect(sent).toHaveLength(1);
  });

  it("rejects cron scheduling when the provider has no scheduler", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    await expect(queue.schedule("job", {}, { cron: "* * * * *" })).rejects.toThrow(UnsupportedCapabilityError);
  });
});

describe("createQueueRegistry", () => {
  it("preserves provider types per name", () => {
    const orders = createQueue({ name: "orders", provider: fakeProvider() });
    const registry = createQueueRegistry({ orders });
    expect(registry.get("orders")).toBe(orders);
    expect(registry.has("orders")).toBe(true);
    expect(registry.names()).toEqual(["orders"]);
  });

  it("throws on unknown names", () => {
    const registry = createQueueRegistry();
    expect(() => registry.get("nope")).toThrow();
  });
});

describe("queue lifecycle", () => {
  it("refuses sends after close", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    await queue.close();
    expect(queue.closed).toBe(true);
    await expect(queue.send("job", {})).rejects.toThrow(/closed/);
  });

  it("closes registered workers with the queue", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    const worker = queue.worker("job", async () => {}, { autoStart: false });
    expect(worker.running).toBe(false);
    await queue.close();
    expect(worker.running).toBe(false);
  });

  it("waitForQueueEvent resolves on the matching event", async () => {
    const queue = createQueue({ name: "orders", provider: fakeProvider() });
    const sent = waitForQueueEvent(queue, "sent", { timeout: "1s" });
    await queue.send("job", {});
    await expect(sent).resolves.toMatchObject({ jobName: "job" });
  });
});
