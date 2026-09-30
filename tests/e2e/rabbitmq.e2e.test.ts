import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { createQueue } from "../../src/index.js";
import type { RabbitMqProvider } from "../../src/drivers/rabbitmq.js";
import { RABBITMQ_URL, deferred, reachable, skipHint, unique, until } from "./helpers.js";

const up = await reachable(RABBITMQ_URL);
if (!up) console.warn(skipHint(RABBITMQ_URL));

describe.skipIf(!up)("rabbitmq provider end to end", () => {
  let provider: RabbitMqProvider;
  const queues: string[] = [];

  beforeAll(async () => {
    provider = await createQueue({ type: "rabbitmq", url: RABBITMQ_URL });
  }, 20_000);
  afterAll(async () => {
    for (const queue of queues) {
      try {
        await provider.native().channel?.deleteQueue(queue);
      } catch {
        /* queue may already be gone */
      }
    }
    await provider?.close();
  });

  it("round-trips a message with manual acknowledgement", { timeout: 30_000 }, async () => {
    const queue = unique("queuekit.e2e.ack");
    queues.push(queue);
    const received = deferred<{
      payload: { orderId: string };
      correlationId?: string;
      type?: string;
    }>();
    const consumer = await provider.consume<{ orderId: string }>(
      queue,
      async ({ message, ack }) => {
        await ack.complete();
        received.resolve({
          payload: message.payload,
          correlationId: message.correlationId,
          type: message.type,
        });
      },
    );
    const result = await provider.publish(queue, {
      id: "ord-1",
      type: "order.placed",
      payload: { orderId: "ord-1" },
      correlationId: "corr-9",
    });

    const message = await received.promise;
    expect(result.ok).toBe(true);
    expect(message.payload).toEqual({ orderId: "ord-1" });
    expect(message.type).toBe("order.placed");
    expect(message.correlationId).toBe("corr-9");
    await until(async () =>
      expect(await provider.native().channel!.checkQueue(queue)).toMatchObject({ messageCount: 0 }),
    );
    await consumer.close();
  });

  it("auto-acknowledges when autoAck is set", { timeout: 30_000 }, async () => {
    const queue = unique("queuekit.e2e.autoack");
    queues.push(queue);
    const received = deferred<{ text: string }>();
    const consumer = await provider.consume<{ text: string }>(
      queue,
      ({ message }) => received.resolve(message.payload),
      { autoAck: true },
    );
    await provider.publish(queue, { payload: { text: "auto" } });

    await expect(received.promise).resolves.toEqual({ text: "auto" });
    await until(async () =>
      expect(await provider.native().channel!.checkQueue(queue)).toMatchObject({ messageCount: 0 }),
    );
    await consumer.close();
  });

  it("requeues a retry and redelivers the message", { timeout: 30_000 }, async () => {
    const queue = unique("queuekit.e2e.retry");
    queues.push(queue);
    const deliveries: boolean[] = []; // native.fields.redelivery per delivery
    const settled = deferred<void>();
    const consumer = await provider.consume<{ attempt: number }>(
      queue,
      async ({ message, ack }) => {
        deliveries.push(message.native.fields.redelivered === true);
        if (deliveries.length < 2) await ack.retry?.();
        else {
          await ack.complete();
          settled.resolve();
        }
      },
    );
    await provider.publish(queue, { payload: { attempt: 1 } });

    await settled.promise;
    expect(deliveries).toEqual([false, true]);
    await until(async () =>
      expect(await provider.native().channel!.checkQueue(queue)).toMatchObject({ messageCount: 0 }),
    );
    await consumer.close();
  });
});
