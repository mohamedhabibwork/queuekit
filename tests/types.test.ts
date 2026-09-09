import { createQueue, createQueueManager, createTypedQueue } from '../src/index.js';

async function typeExamples(): Promise<void> {
  const kafka = await createQueue({ type: 'kafka', clientId: 'app', brokers: ['localhost:9092'] });
  await kafka.publish('events', { payload: { id: '1' } }, { native: { partition: 1, headers: { source: 'test' } } });
  const jobs = await createQueue({ type: 'bullmq', connection: { host: 'localhost', port: 6379 } });
  await jobs.publish('emails', { payload: { id: '1' } }, { native: { attempts: 3, backoff: { type: 'exponential', delay: 100 } } });
  const queues = createTypedQueue<{ 'email.send': { payload: { readonly to: string } } }>(jobs);
  await queues.publish('email.send', { to: 'person@example.com' });
  const manager = createQueueManager({ providers: { kafka: { type: 'kafka', clientId: 'api', brokers: ['localhost:9092'] }, jobs: { type: 'bullmq', connection: { host: 'localhost', port: 6379 } } } });
  const provider = await manager.provider('kafka');
  await provider.publish('events', { payload: {} });
}
void typeExamples;
