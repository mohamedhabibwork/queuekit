import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { createQueue } from "../../src/index.js";
import type { RedisProvider } from "../../src/drivers/redis.js";
import type { ConsumedMessage } from "../../src/core/types.js";
import { REDIS_URL, VALKEY_URL, deferred, reachable, skipHint, unique, until } from "./helpers.js";

const up = await reachable(REDIS_URL);
if (!up) console.warn(skipHint(REDIS_URL));

describe.skipIf(!up)("redis provider end to end", () => {
  let provider: RedisProvider;

  beforeAll(async () => {
    provider = await createQueue({ type: "redis", mode: "streams", url: REDIS_URL, group: "e2e" });
  }, 20_000);
  afterAll(async () => {
    await provider?.close();
  });

  it(
    "round-trips a message through streams with manual acknowledgement and clears the pending list",
    { timeout: 30_000 },
    async () => {
      const stream = unique("queuekit-e2e-stream");
      const received = deferred<ConsumedMessage<{ invoiceId: string }>>();
      const consumer = await provider.consume<{ invoiceId: string }>(
        stream,
        async ({ message, ack }) => {
          await ack.complete();
          received.resolve(message);
        },
      );
      const result = await provider.publish(stream, {
        id: "inv-1",
        type: "invoice.created",
        payload: { invoiceId: "inv-1" },
        correlationId: "corr-1",
        headers: { locale: "en" },
      });

      const message = await received.promise;
      expect(result.ok).toBe(true);
      expect(result.messageId).toMatch(/^\d+-\d+$/); // XADD entry id
      expect(message.payload).toEqual({ invoiceId: "inv-1" });
      expect(message.type).toBe("invoice.created");
      expect(message.correlationId).toBe("corr-1");
      expect(message.headers).toEqual({ locale: "en" });
      expect(message.attempt).toBe(1);

      await until(async () => {
        const pending = await provider.native()!.sendCommand<unknown>(["XPENDING", stream, "e2e"]);
        expect(Array.isArray(pending) ? pending[0] : pending).toBe(0);
      });
      await consumer.close();
    },
  );

  it(
    "delivers a backlog published before the consumer group existed",
    { timeout: 30_000 },
    async () => {
      const stream = unique("queuekit-e2e-backlog");
      await provider.publish(stream, { payload: { n: 1 } });
      await provider.publish(stream, { payload: { n: 2 } });

      const got: number[] = [];
      const consumer = await provider.consume<{ n: number }>(stream, async ({ message, ack }) => {
        got.push(message.payload.n);
        await ack.complete();
      });
      await until(() => expect(got.toSorted()).toEqual([1, 2]));
      await consumer.close();
    },
  );

  it("requeues a retry with the next attempt and redelivers it", { timeout: 30_000 }, async () => {
    const stream = unique("queuekit-e2e-retry");
    const attempts: number[] = [];
    const settled = deferred<void>();
    const consumer = await provider.consume<{ n: number }>(stream, async ({ message, ack }) => {
      attempts.push(message.attempt ?? 1);
      if ((message.attempt ?? 1) < 2) await ack.retry?.();
      else {
        await ack.complete();
        settled.resolve();
      }
    });
    await provider.publish(stream, { payload: { n: 7 } });

    await settled.promise;
    expect(attempts).toEqual([1, 2]);
    await until(async () => {
      const pending = await provider.native()!.sendCommand<unknown>(["XPENDING", stream, "e2e"]);
      expect(Array.isArray(pending) ? pending[0] : pending).toBe(0);
    });
    await consumer.close();
  });

  it(
    "dead-letters a rejected entry to the configured stream and clears the pending list",
    { timeout: 30_000 },
    async () => {
      const stream = unique("queuekit-e2e-dlq");
      const deadLetter = `${stream}:dead`;
      const consumer = await provider.consume<{ poison: boolean }>(
        stream,
        async ({ ack }) => {
          await ack.reject?.(); // no requeue → dead-letter, then acknowledge
        },
        { native: { deadLetter } },
      );
      await provider.publish(stream, { id: "poison-1", payload: { poison: true } });

      const client = provider.native()!;
      await until(async () => {
        expect(await client.sendCommand<number>(["XLEN", deadLetter])).toBe(1);
        const pending = await client.sendCommand<unknown>(["XPENDING", stream, "e2e"]);
        expect(Array.isArray(pending) ? pending[0] : pending).toBe(0);
      });
      await consumer.close();

      // The dead-letter entry keeps the payload and records where it came from.
      const range = await client.sendCommand<unknown>(["XRANGE", deadLetter, "-", "+"]);
      const raw = Array.isArray(range) ? (range[0] as unknown[])[1] : null;
      const fields = Array.isArray(raw) ? raw : [];
      const index = fields.indexOf("data");
      const entry = JSON.parse(String(fields[index + 1]));
      expect(entry.payload).toEqual({ poison: true });
      expect(entry.attempt).toBe(1);
      expect(entry.deadLetterOf).toMatchObject({ stream, group: "e2e" });
    },
  );

  it("round-trips a message through pub/sub", { timeout: 30_000 }, async () => {
    const pubsub = await createQueue<{ type: "redis"; mode: "pubsub"; url: string }>({
      type: "redis",
      mode: "pubsub",
      url: REDIS_URL,
    });
    const channel = unique("queuekit-e2e-channel");
    const received = deferred<ConsumedMessage<{ temperature: number }>>();
    const consumer = await pubsub.consume<{ temperature: number }>(channel, ({ message }) =>
      received.resolve(message),
    );

    const result = await pubsub.publish(channel, {
      type: "sensor.reading",
      payload: { temperature: 21 },
    });
    const message = await received.promise;
    expect(result.ok).toBe(true);
    expect(result.native).toBe(1); // one subscriber received the fan-out
    expect(message.payload).toEqual({ temperature: 21 });
    expect(message.type).toBe("sensor.reading");

    await consumer.close();
    await pubsub.close();
  });
});

const valkeyUp = await reachable(VALKEY_URL);
if (!valkeyUp) console.warn(skipHint(VALKEY_URL));

describe.skipIf(!valkeyUp)("redis provider via ioredis against a valkey server", () => {
  let provider: RedisProvider;

  beforeAll(async () => {
    provider = await createQueue({
      type: "redis",
      mode: "streams",
      url: VALKEY_URL,
      group: "e2e",
      client: "ioredis",
    });
  }, 20_000);
  afterAll(async () => {
    await provider?.close();
  });

  it("round-trips a streams message with manual acknowledgement", { timeout: 30_000 }, async () => {
    const stream = unique("queuekit-e2e-ioredis");
    const received = deferred<ConsumedMessage<{ paymentId: string }>>();
    const consumer = await provider.consume<{ paymentId: string }>(
      stream,
      async ({ message, ack }) => {
        await ack.complete();
        received.resolve(message);
      },
    );
    const result = await provider.publish(stream, {
      payload: { paymentId: "pay-7" },
      correlationId: "corr-7",
    });

    const message = await received.promise;
    expect(result.ok).toBe(true);
    expect(result.messageId).toMatch(/^\d+-\d+$/);
    expect(message.payload).toEqual({ paymentId: "pay-7" });
    expect(message.correlationId).toBe("corr-7");

    await until(async () => {
      const pending = await provider.native()!.sendCommand<unknown>(["XPENDING", stream, "e2e"]);
      expect(Array.isArray(pending) ? pending[0] : pending).toBe(0);
    });
    await consumer.close();
  });

  it("requeues a retry with the next attempt", { timeout: 30_000 }, async () => {
    const stream = unique("queuekit-e2e-ioredis-retry");
    const attempts: number[] = [];
    const settled = deferred<void>();
    const consumer = await provider.consume<{ n: number }>(stream, async ({ message, ack }) => {
      attempts.push(message.attempt ?? 1);
      if ((message.attempt ?? 1) < 2) await ack.retry?.();
      else {
        await ack.complete();
        settled.resolve();
      }
    });
    await provider.publish(stream, { payload: { n: 9 } });

    await settled.promise;
    expect(attempts).toEqual([1, 2]);
    await consumer.close();
  });

  it("dead-letters a rejected entry", { timeout: 30_000 }, async () => {
    const stream = unique("queuekit-e2e-ioredis-dlq");
    const deadLetter = `${stream}:dead`;
    const consumer = await provider.consume<{ bad: boolean }>(
      stream,
      async ({ ack }) => {
        await ack.reject?.();
      },
      { native: { deadLetter } },
    );
    await provider.publish(stream, { payload: { bad: true } });

    const client = provider.native()!;
    await until(async () => {
      expect(await client.sendCommand<number>(["XLEN", deadLetter])).toBe(1);
      const pending = await client.sendCommand<unknown>(["XPENDING", stream, "e2e"]);
      expect(Array.isArray(pending) ? pending[0] : pending).toBe(0);
    });
    await consumer.close();
  });

  it("round-trips a message through pub/sub", { timeout: 30_000 }, async () => {
    const pubsub = await createQueue({
      type: "redis",
      mode: "pubsub",
      url: VALKEY_URL,
      client: "ioredis",
    });
    const channel = unique("queuekit-e2e-ioredis-channel");
    const received = deferred<ConsumedMessage<{ temperature: number }>>();
    const consumer = await pubsub.consume<{ temperature: number }>(channel, ({ message }) =>
      received.resolve(message),
    );

    const result = await pubsub.publish(channel, { payload: { temperature: 30 } });
    const message = await received.promise;
    expect(result.ok).toBe(true);
    expect(result.native).toBe(1);
    expect(message.payload).toEqual({ temperature: 30 });

    await consumer.close();
    await pubsub.close();
  });
});
