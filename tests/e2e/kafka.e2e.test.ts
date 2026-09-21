import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { createQueue } from '../../src/index.js';
import type { KafkaProvider } from '../../src/drivers/kafka.js';
import { KAFKA_BROKER, deferred, portOpen, skipHint, unique } from './helpers.js';

const [host, port] = KAFKA_BROKER.split(':');
const up = await portOpen(Number(port) || 9092, host);
if (!up) console.warn(skipHint(`kafka://${KAFKA_BROKER}`));

describe.skipIf(!up)('kafka provider end to end', () => {
  let provider: KafkaProvider;
  const clientId = unique('queuekit-e2e');

  beforeAll(async () => { provider = await createQueue({ type: 'kafka', clientId, brokers: [KAFKA_BROKER] }); }, 60_000);
  afterAll(async () => { await provider?.close(); });

  // Fresh topic per test: a `fromBeginning` consumer with a new group replays
  // everything on the topic, so topics must not be shared between tests.
  async function freshTopic(): Promise<string> {
    const topic = unique('queuekit-e2e-topic').replace(/[^a-zA-Z0-9._-]/g, '-');
    const { Kafka } = await import('kafkajs');
    const admin = new Kafka({ clientId, brokers: [KAFKA_BROKER] }).admin();
    await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
    await admin.disconnect();
    return topic;
  }

  it('round-trips a message with offset commit on acknowledge', { timeout: 60_000 }, async () => {
    const topic = await freshTopic();
    const groupId = unique('queuekit-e2e-group').replace(/[^a-zA-Z0-9._-]/g, '-');
    const received = deferred<{ payload: { shipmentId: string }; headers: Record<string, unknown> }>();
    const consumer = await provider.consume<{ shipmentId: string }>(topic, async ({ message, ack }) => {
      await ack.complete(); // commits the offset
      received.resolve({ payload: message.payload, headers: message.headers });
    }, { native: { groupId, fromBeginning: true } });

    const result = await provider.publish(topic, { id: 'ship-1', type: 'shipment.dispatched', payload: { shipmentId: 'ship-1' }, traceId: 'trace-1', headers: { warehouse: 'eu-1' } });
    const message = await received.promise;

    expect(result.ok).toBe(true);
    expect(result.partition).toBe(0);
    expect(result.offset).toBeDefined();
    expect(message.payload).toEqual({ shipmentId: 'ship-1' });
    expect(message.headers).toEqual({ warehouse: 'eu-1' });

    const { Kafka } = await import('kafkajs');
    const admin = new Kafka({ clientId, brokers: [KAFKA_BROKER] }).admin();
    const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
    await admin.disconnect();
    expect(offsets[0]?.partitions[0]?.offset).toBe('1'); // one message committed past offset 0
    await consumer.close();
  });

  it('auto-acknowledges when autoAck is set', { timeout: 60_000 }, async () => {
    const topic = await freshTopic();
    const groupId = unique('queuekit-e2e-auto-group').replace(/[^a-zA-Z0-9._-]/g, '-');
    const received = deferred<{ n: number }>();
    const consumer = await provider.consume<{ n: number }>(topic, ({ message }) => received.resolve(message.payload), { native: { groupId, fromBeginning: true }, autoAck: true });
    await provider.publish(topic, { payload: { n: 42 } });

    await expect(received.promise).resolves.toEqual({ n: 42 });
    await consumer.close();
  });
});
