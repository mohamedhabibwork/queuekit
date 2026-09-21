import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { createQueue } from '../../src/index.js';
import type { NatsProvider } from '../../src/drivers/nats.js';
import { NATS_URL, deferred, reachable, skipHint, unique, until } from './helpers.js';

const up = await reachable(NATS_URL);
if (!up) console.warn(skipHint(NATS_URL));

describe.skipIf(!up)('nats provider end to end', () => {
  let provider: NatsProvider;

  beforeAll(async () => { provider = await createQueue({ type: 'nats', servers: [NATS_URL], mode: 'core' }); }, 20_000);
  afterAll(async () => { await provider?.close(); });

  it('delivers a published message to a live subscriber', { timeout: 30_000 }, async () => {
    const subject = unique('queuekit.e2e.events'); // core NATS delivers only to live subscribers
    const received = deferred<{ payload: { userId: string }; headers: Record<string, unknown> }>();
    const consumer = await provider.consume<{ userId: string }>(subject, ({ message }) => {
      received.resolve({ payload: message.payload, headers: message.headers });
    });
    const result = await provider.publish(subject, { type: 'user.signed-up', payload: { userId: 'u-1' }, headers: { plan: 'pro' } });

    const message = await received.promise;
    expect(result.ok).toBe(true);
    expect(message.payload).toEqual({ userId: 'u-1' });
    expect(message.headers).toEqual({ plan: 'pro' });
    await consumer.close();
  });

  it('load-balances across a queue group with no duplicates', { timeout: 30_000 }, async () => {
    const subject = unique('queuekit.e2e.work');
    const queue = 'workers';
    const first: string[] = []; const second: string[] = [];
    const a = await provider.consume<{ n: string }>(subject, ({ message }) => { first.push(message.payload.n); }, { native: { queue } });
    const b = await provider.consume<{ n: string }>(subject, ({ message }) => { second.push(message.payload.n); }, { native: { queue } });

    await provider.publish(subject, { payload: { n: 'one' } });
    await provider.publish(subject, { payload: { n: 'two' } });

    await until(() => expect([...first, ...second].sort()).toEqual(['one', 'two']));
    await a.close(); await b.close();
  });
});
