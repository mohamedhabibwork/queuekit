import { test, expect } from 'bun:test';
import { createMemoryQueue } from '../../src/testing.ts';

test('QueueKit memory provider runs on Bun', async () => {
  const queue = createMemoryQueue();
  const result = await queue.publish('smoke', { payload: { runtime: 'bun' } });
  expect(result.ok).toBe(true);
  await queue.close();
});
