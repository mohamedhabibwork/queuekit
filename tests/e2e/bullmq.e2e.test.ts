import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { createQueue } from '../../src/index.js';
import type { BullMqProvider } from '../../src/drivers/bullmq.js';
import { REDIS_URL, deferred, reachable, skipHint, unique, until } from './helpers.js';

const up = await reachable(REDIS_URL);
if (!up) console.warn(skipHint(REDIS_URL));

describe.skipIf(!up)('bullmq provider end to end', () => {
  let provider: BullMqProvider;

  beforeAll(async () => { provider = await createQueue({ type: 'bullmq', connection: REDIS_URL }); }, 20_000);
  afterAll(async () => { await provider?.close(); });

  it('processes a published job with its payload intact', { timeout: 30_000 }, async () => {
    const queueName = unique('queuekit-e2e-jobs');
    const received = deferred<{ payload: { invoiceId: string }; jobId?: string; type?: string }>();
    const consumer = await provider.consume<{ invoiceId: string }>(queueName, ({ message }) => {
      received.resolve({ payload: message.payload, jobId: message.id, type: message.type });
    });
    const result = await provider.publish(queueName, { type: 'invoice.render', payload: { invoiceId: 'inv-9' } });

    const message = await received.promise;
    expect(result.ok).toBe(true);
    expect(result.messageId).toBeDefined();
    expect(message.payload).toEqual({ invoiceId: 'inv-9' });
    expect(message.jobId).toBe(result.messageId); // BullMQ job id flows through
    expect(message.type).toBe('invoice.render');
    await consumer.close();
  });

  it('processes a publishMany batch exactly once (BullMQ does not guarantee FIFO order)', { timeout: 30_000 }, async () => {
    const queueName = unique('queuekit-e2e-batch');
    const processed: number[] = [];
    const consumer = await provider.consume<{ n: number }>(queueName, ({ message }) => { processed.push(message.payload.n); }, { concurrency: 1 });
    const results = await provider.publishMany<{ n: number }>(queueName, [{ payload: { n: 1 } }, { payload: { n: 2 } }, { payload: { n: 3 } }]);

    expect(results).toHaveLength(3);
    expect(results.every((result) => result.ok)).toBe(true);
    await until(() => expect(processed.length).toBe(3));
    expect([...processed].sort()).toEqual([1, 2, 3]);
    await consumer.close();
  });
});
