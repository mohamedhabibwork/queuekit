import type { ConsumerStatus, QueueConsumer } from './types.js';

export function createConsumer(controls: { pause?: () => Promise<void>; resume?: () => Promise<void>; close: () => Promise<void> }): QueueConsumer {
  let status: ConsumerStatus = 'running';
  return {
    get status() { return status; },
    async pause() { if (status === 'running') { await controls.pause?.(); status = 'paused'; } },
    async resume() { if (status === 'paused') { await controls.resume?.(); status = 'running'; } },
    async close() { if (status !== 'closed' && status !== 'closing') { status = 'closing'; await controls.close(); status = 'closed'; } },
  };
}
