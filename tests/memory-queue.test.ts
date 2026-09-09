import { describe, expect, it } from 'vitest';
import { createMemoryQueue } from '../src/testing.js';

describe('MemoryQueue', () => {
  it('stores published messages and delivers them to consumers', async () => {
    const queue = createMemoryQueue();
    const received = new Promise<{ readonly email: string }>((resolve) => {
      void queue.consume<{ email: string }>('emails', ({ message }) => resolve(message.payload));
    });
    const result = await queue.publish('emails', { type: 'welcome', payload: { email: 'person@example.com' } });
    expect(result).toMatchObject({ ok: true, provider: 'memory', messageId: 'memory-1' });
    await expect(received).resolves.toEqual({ email: 'person@example.com' });
    expect(queue.lastMessage('emails')).toMatchObject({ type: 'welcome' });
  });

  it('runs middleware and closes idempotently', async () => {
    const queue = createMemoryQueue(); const operations: string[] = [];
    queue.use(async (context, next) => { operations.push(context.operation); await next(); });
    await queue.publish('events', { payload: { id: 'evt-1' } });
    expect(operations).toEqual(['publish']);
    await queue.close(); await queue.close();
    await expect(queue.publish('events', { payload: {} })).rejects.toThrow('closed');
  });
});
