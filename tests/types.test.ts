import { createQueue, createQueueManager, createTypedQueue } from "../src/index.js";

async function typeExamples(): Promise<void> {
  const kafka = await createQueue({ type: "kafka", clientId: "app", brokers: ["localhost:9092"] });
  await kafka.publish(
    "events",
    { payload: { id: "1" } },
    { native: { partition: 1, headers: { source: "test" } } },
  );
  const jobs = await createQueue({ type: "bullmq", connection: { host: "localhost", port: 6379 } });
  await jobs.publish(
    "emails",
    { payload: { id: "1" } },
    { native: { attempts: 3, backoff: { type: "exponential", delay: 100 } } },
  );
  const queues = createTypedQueue<{ "email.send": { payload: { readonly to: string } } }>(jobs);
  await queues.publish("email.send", { to: "person@example.com" });
  const manager = createQueueManager({
    providers: {
      kafka: { type: "kafka", clientId: "api", brokers: ["localhost:9092"] },
      jobs: { type: "bullmq", connection: { host: "localhost", port: 6379 } },
    },
  });
  const provider = await manager.provider("kafka");
  await provider.publish("events", { payload: {} });
  const fake = await createQueue({ type: "memory" });
  await fake.publish("events", { payload: { id: "1" } }, { delay: 100 });
  const pendingIds: Array<string | undefined> = fake.pending("events").map((message) => message.id);
  void pendingIds;
  const cloudflare = await createQueue({
    type: "cloudflare",
    queueId: "queue-id",
    accountId: "account",
    apiToken: "token",
  });
  await cloudflare.publish("orders", { payload: {} }, { native: { delaySeconds: 5 } });
  const azure = await createQueue({
    type: "azureservicebus",
    connectionString: "Endpoint=sb://example/;SharedAccessKey=k",
  });
  await azure.publish("jobs", { payload: {} }, { delay: 1_000 });
  const gcp = await createQueue({ type: "gcpubsub", projectId: "p", topic: "events" });
  await gcp.publish("events", { payload: {} }, { native: { orderingKey: "k" } });
  const managed = createQueueManager({
    providers: {
      push: { type: "cloudflare", queueId: "queue-id", accountId: "a", apiToken: "t" },
      bus: { type: "azureservicebus", connectionString: "Endpoint=sb://x/;SharedAccessKey=k" },
      fanout: { type: "gcpubsub", projectId: "p", topic: "t" },
    },
  });
  const bus = await managed.provider("bus");
  await bus.publish("jobs", { payload: {} });
  void managed;
}
void typeExamples;
