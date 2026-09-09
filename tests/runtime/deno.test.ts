import { createMemoryQueue } from '../../dist/testing.js';

Deno.test('QueueKit memory provider runs on Deno', async () => {
  const queue = createMemoryQueue();
  const result: { readonly ok: boolean } = await queue.publish('smoke', { payload: { runtime: 'deno' } });
  if (!result.ok) throw new Error('Memory provider did not publish.');
  await queue.close();
});
