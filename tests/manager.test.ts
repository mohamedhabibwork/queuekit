import { describe, expect, it } from 'vitest';
import { createQueueManager } from '../src/index.js';

describe('QueueManager', () => {
  it('does not initialize providers until requested', async () => {
    const manager = createQueueManager({ providers: { events: { type: 'nats', servers: 'nats://localhost:4222' }, cache: { type: 'redis', mode: 'pubsub', url: 'redis://localhost:6379' } }, default: 'events' });
    const provider = await manager.provider('events');
    expect(provider.name).toBe('nats');
    await manager.close();
  });
});
