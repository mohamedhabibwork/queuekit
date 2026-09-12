import { describe, expect, it } from 'vitest';
import { createFakeQueue, createMemoryQueue, MemoryQueue } from '../src/testing.js';
import { createQueue, createQueueManager } from '../src/index.js';

describe('memory fake driver', () => {
  it('delivers backlog messages to consumers that attach later', async () => {
    const queue = createMemoryQueue();
    await queue.publish('emails', { type: 'welcome', payload: { email: 'person@example.com' } });
    expect(queue.pending('emails')).toHaveLength(1);
    const received = new Promise<string>((resolve) => {
      void queue.consume<{ email: string }>('emails', ({ message }) => resolve(message.payload.email));
    });
    await expect(received).resolves.toBe('person@example.com');
    expect(queue.acknowledged('emails')).toHaveLength(1);
    expect(queue.pending('emails')).toHaveLength(0);
  });

  it('waits for in-flight handler work with waitUntilIdle', async () => {
    const queue = createMemoryQueue();
    const processed: string[] = [];
    await queue.consume<{ id: string }>('jobs', async ({ message }) => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      processed.push(message.payload.id);
    });
    await queue.publishMany('jobs', [{ payload: { id: 'a' } }, { payload: { id: 'b' } }]);
    expect(processed).toEqual([]);
    await queue.waitUntilIdle();
    expect(processed).toEqual(['a', 'b']);
  });

  it('splits a backlog across competing consumers', async () => {
    const queue = createMemoryQueue();
    await queue.publishMany<{ id: string }>('jobs', [{ payload: { id: '1' } }, { payload: { id: '2' } }, { payload: { id: '3' } }, { payload: { id: '4' } }]);
    const first: string[] = [];
    const second: string[] = [];
    await queue.consume<{ id: string }>('jobs', async ({ message }) => { first.push(message.payload.id); });
    await queue.consume<{ id: string }>('jobs', async ({ message }) => { second.push(message.payload.id); });
    await queue.waitUntilIdle();
    expect([...first, ...second].sort()).toEqual(['1', '2', '3', '4']);
    expect(first.length).toBeGreaterThan(0);
    expect(second.length).toBeGreaterThan(0);
  });

  it('holds delayed messages until they are due or flushed', async () => {
    const queue = createMemoryQueue();
    const received: string[] = [];
    await queue.consume<{ id: string }>('delayed', async ({ message }) => { received.push(message.payload.id); });
    await queue.publish('delayed', { payload: { id: 'later' } }, { delay: 60_000 });
    await queue.publish('delayed', { payload: { id: 'scheduled' } }, { scheduleAt: new Date(Date.now() + 60_000) });
    await queue.waitUntilIdle();
    expect(received).toEqual([]);
    expect(queue.pending('delayed')).toHaveLength(2);
    await queue.flush();
    expect(received.sort()).toEqual(['later', 'scheduled']);
    expect(queue.pending('delayed')).toHaveLength(0);
  });

  it('requeues retried messages with an incremented attempt', async () => {
    const queue = createMemoryQueue();
    const attempts: number[] = [];
    await queue.consume<{ id: string }>('jobs', async ({ message, ack }) => {
      attempts.push(message.attempt ?? 0);
      if ((message.attempt ?? 0) < 2) await ack.retry?.({ delay: 0 });
      else await ack.complete();
    }, { autoAck: false });
    await queue.publish('jobs', { payload: { id: 'j1' } });
    await queue.waitUntilIdle();
    expect(attempts).toEqual([1, 2]);
    expect(queue.acknowledged('jobs')).toHaveLength(1);
    expect(queue.pending('jobs')).toHaveLength(0);
  });

  it('moves rejected messages to dead letters, or requeues on request', async () => {
    const queue = createMemoryQueue();
    const deliveries: string[] = [];
    const seen = new Set<string>();
    await queue.consume<{ id: string }>('jobs', async ({ message, ack }) => {
      deliveries.push(message.payload.id);
      if (!seen.has(message.payload.id)) { seen.add(message.payload.id); await ack.reject?.({ requeue: true }); }
      else await ack.reject?.();
    }, { autoAck: false });
    await queue.publish('jobs', { payload: { id: 'poison' } });
    await queue.waitUntilIdle();
    expect(deliveries).toEqual(['poison', 'poison']);
    expect(queue.deadLetters('jobs')).toHaveLength(1);
    expect(queue.acknowledged('jobs')).toHaveLength(0);
  });

  it('retries throwing handlers up to maxAttempts then dead-letters', async () => {
    const queue = createMemoryQueue();
    queue.setMaxAttempts(2);
    const attempts: number[] = [];
    await queue.consume<{ id: string }>('jobs', async ({ message }) => {
      attempts.push(message.attempt ?? 0);
      throw new Error('boom');
    });
    await queue.publish('jobs', { payload: { id: 'x' } });
    await queue.waitUntilIdle();
    expect(attempts).toEqual([1, 2]);
    expect(queue.deadLetters('jobs')).toHaveLength(1);
    expect(queue.failures('jobs')).toHaveLength(2);
    expect(queue.failures('jobs').at(0)?.error).toBeInstanceOf(Error);
    expect(queue.acknowledged('jobs')).toHaveLength(0);
  });

  it('keeps messages pending until acknowledged when autoAck is disabled', async () => {
    const queue = createMemoryQueue();
    let held: { readonly attempt?: number; readonly ack: { complete(): Promise<void> } } | undefined;
    await queue.consume<{ id: string }>('jobs', async ({ message, ack }) => { held = { attempt: message.attempt, ack }; }, { autoAck: false });
    await queue.publish('jobs', { payload: { id: 'hold' } });
    await queue.waitUntilIdle();
    expect(queue.pending('jobs')).toHaveLength(1);
    expect(held?.attempt).toBe(1);
    await held!.ack.complete();
    expect(queue.acknowledged('jobs')).toHaveLength(1);
    expect(queue.pending('jobs')).toHaveLength(0);
  });

  it('holds delivery while paused and resumes afterwards', async () => {
    const queue = createMemoryQueue();
    const received: number[] = [];
    await queue.consume<number>('jobs', async ({ message }) => { received.push(message.payload); });
    queue.pause();
    await queue.publish('jobs', { payload: 1 });
    await queue.waitUntilIdle();
    expect(received).toEqual([]);
    queue.resume();
    await queue.waitUntilIdle();
    expect(received).toEqual([1]);
  });

  it('supports publish failure injection and clearing', async () => {
    const queue = createMemoryQueue();
    queue.failNext(new Error('broker down'));
    await expect(queue.publish('events', { payload: {} })).rejects.toThrow('broker down');
    expect(queue.messages('events')).toHaveLength(0);
    await expect(queue.publish('events', { payload: {} })).resolves.toMatchObject({ ok: true });
    queue.clear();
    expect(queue.messages('events')).toHaveLength(0);
    expect(queue.acknowledged('events')).toHaveLength(0);
  });

  it('creates fake queues with createFakeQueue from the testing module', async () => {
    const queue = createFakeQueue();
    const received = new Promise<string>((resolve) => {
      void queue.consume<{ id: string }>('jobs', ({ message }) => resolve(message.payload.id));
    });
    await queue.publish('jobs', { payload: { id: 'fake-1' } });
    await expect(received).resolves.toBe('fake-1');
    expect(queue).toBeInstanceOf(MemoryQueue);
    expect(await createQueue({ type: 'memory' })).toBeInstanceOf(MemoryQueue);
  });

  it('works through createQueue and createQueueManager', async () => {
    const provider = await createQueue({ type: 'memory' });
    expect(provider.name).toBe('memory');
    const manager = createQueueManager({ providers: { jobs: { type: 'memory' } }, default: 'jobs' });
    const jobs = await manager.default();
    const received = new Promise<string>((resolve) => {
      void jobs.consume<{ id: string }>('jobs', ({ message }) => resolve(message.payload.id));
    });
    await jobs.publish('jobs', { payload: { id: 'via-manager' } });
    await expect(received).resolves.toBe('via-manager');
    expect((await manager.health()).jobs).toMatchObject({ ok: true, provider: 'memory' });
    await manager.close();
  });
});
