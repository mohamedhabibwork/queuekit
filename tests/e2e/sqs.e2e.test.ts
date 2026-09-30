import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { createQueue } from "../../src/index.js";
import type { SqsProvider } from "../../src/drivers/sqs.js";
import { SQS_ENDPOINT, deferred, reachable, skipHint, unique, until } from "./helpers.js";

const up = await reachable(SQS_ENDPOINT);
if (!up) console.warn(skipHint(SQS_ENDPOINT));

describe.skipIf(!up)("sqs provider end to end (localstack)", () => {
  let provider: SqsProvider;
  let sqs: import("@aws-sdk/client-sqs").SQSClient;
  const queueUrls: string[] = [];

  beforeAll(async () => {
    provider = await createQueue({
      type: "sqs",
      region: "us-east-1",
      endpoint: SQS_ENDPOINT,
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    const { SQSClient } = await import("@aws-sdk/client-sqs");
    sqs = new SQSClient({
      region: "us-east-1",
      endpoint: SQS_ENDPOINT,
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
  }, 20_000);
  afterAll(async () => {
    const { DeleteQueueCommand } = await import("@aws-sdk/client-sqs");
    for (const url of queueUrls) {
      try {
        await sqs.send(new DeleteQueueCommand({ QueueUrl: url }));
      } catch {
        /* already gone */
      }
    }
    await provider?.close();
    sqs?.destroy();
  });

  async function freshQueue(): Promise<string> {
    const { CreateQueueCommand } = await import("@aws-sdk/client-sqs");
    const name = unique("queuekit-e2e"); // SQS names: alphanumerics, hyphens, underscores
    const response = await sqs.send(new CreateQueueCommand({ QueueName: name }));
    queueUrls.push(response.QueueUrl!);
    return response.QueueUrl!;
  }

  async function messageCount(queueUrl: string): Promise<number> {
    const { GetQueueAttributesCommand } = await import("@aws-sdk/client-sqs");
    const response = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
      }),
    );
    return (
      Number(response.Attributes?.ApproximateNumberOfMessages ?? 0) +
      Number(response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0)
    );
  }

  it(
    "round-trips a message with manual acknowledgement deleting it from the queue",
    { timeout: 30_000 },
    async () => {
      const queueUrl = await freshQueue();
      const received = deferred<{ payload: { reportId: string }; messageId?: string }>();
      const consumer = await provider.consume<{ reportId: string }>(
        queueUrl,
        async ({ message, ack }) => {
          await ack.complete(); // deletes the message
          received.resolve({ payload: message.payload, messageId: message.id });
        },
      );
      const result = await provider.publish(queueUrl, {
        id: "rep-1",
        type: "report.ready",
        payload: { reportId: "rep-1" },
      });

      const message = await received.promise;
      expect(result.ok).toBe(true);
      expect(result.messageId).toBeDefined();
      expect(message.payload).toEqual({ reportId: "rep-1" });
      expect(message.messageId).toBeDefined();
      await until(async () => expect(await messageCount(queueUrl)).toBe(0));
      await consumer.close();
    },
  );

  it("auto-acknowledges when autoAck is set", { timeout: 30_000 }, async () => {
    const queueUrl = await freshQueue();
    const received = deferred<{ state: string }>();
    const consumer = await provider.consume<{ state: string }>(
      queueUrl,
      ({ message }) => received.resolve(message.payload),
      { autoAck: true },
    );
    await provider.publish(queueUrl, { payload: { state: "auto-acked" } });

    await expect(received.promise).resolves.toEqual({ state: "auto-acked" });
    await until(async () => expect(await messageCount(queueUrl)).toBe(0));
    await consumer.close();
  });
});
