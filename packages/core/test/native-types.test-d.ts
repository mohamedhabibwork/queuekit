import { describe, expectTypeOf, it } from "vitest";
import {
  createQueue,
  defineJob,
  defineQueueProvider,
  type ProviderTypes,
  type QueueProvider,
} from "../src/index";

/**
 * Compile-time proofs of the core Queue Kit promise:
 *
 *   "native options are inferred from the selected provider, and native
 *    options of a *different* provider are rejected."
 *
 * These use two small fake providers with mutually incompatible native
 * option shapes — the same mechanics the real BullMQ/SQS adapters rely on.
 */

interface RedisLikeTypes extends ProviderTypes {
  connection: { host: string; port: number };
  queue: never;
  send: {
    attempts?: number;
    backoff?: { type: "fixed" | "exponential"; delay: number };
    removeOnComplete?: boolean | number;
    priority?: number;
  };
  receive: never;
  worker: { lockDuration?: number; maxStalledCount?: number };
  nack: never;
  schedule: never;
  messageId: string;
  nativeMessage: { jobId: string };
  nativeClient: { addJob(name: string): Promise<string> };
  nativeResult: { jobId: string };
}

interface AwsLikeTypes extends ProviderTypes {
  connection: { region: string; queueUrl: string };
  queue: never;
  send: {
    MessageGroupId?: string;
    MessageDeduplicationId?: string;
    DelaySeconds?: number;
  };
  receive: { MaxNumberOfMessages?: number; WaitTimeSeconds?: number };
  worker: { waitTimeSeconds?: number; visibilityTimeout?: number; maxNumberOfMessages?: number };
  nack: never;
  schedule: never;
  messageId: string;
  nativeMessage: { receiptHandle: string };
  nativeClient: { sendMessageBatch(): Promise<void> };
  nativeResult: { md5: string };
}

const redisLike = defineQueueProvider((): QueueProvider<RedisLikeTypes> => {
  throw new Error("type test only");
});
const awsLike = defineQueueProvider((): QueueProvider<AwsLikeTypes> => {
  throw new Error("type test only");
});

const redisQueue = createQueue({
  name: "emails",
  provider: redisLike({ connection: { host: "localhost", port: 6379 } }),
});
const awsQueue = createQueue({
  name: "orders",
  provider: awsLike({ connection: { region: "eu-west-1", queueUrl: "https://sqs.local/orders" } }),
});

const sendEmailJob = defineJob<{ to: string; subject: string }, { providerId: string }>("email.send");

describe("native option inference", () => {
  it("accepts redis-like native options on the redis-like queue", async () => {
    const options = {
      delay: "10s" as const,
      native: {
        attempts: 5,
        backoff: { type: "exponential", delay: 1000 } as const,
        removeOnComplete: true,
      },
    };
    // Compiles only when the literal satisfies the inferred native type.
    await redisQueue.send("email.send", {}, options);
  });

  it("accepts SQS-style native options on the aws-like queue", async () => {
    const options = {
      native: {
        MessageGroupId: "customer-42",
        MessageDeduplicationId: "order-123",
        DelaySeconds: 30,
      },
    };
    await awsQueue.send("order.created", {}, options);
  });

  it("rejects SQS native options on the redis-like queue", async () => {
    const options = { native: { MessageGroupId: "abc" } };
    await redisQueue.send(
      "email.send",
      {},
      // @ts-expect-error MessageGroupId is SQS-specific, not a redis-like option
      options,
    );
  });

  it("rejects redis-like native options on the aws-like queue", async () => {
    const options = { native: { removeOnComplete: true } };
    await awsQueue.send(
      "order.created",
      {},
      // @ts-expect-error removeOnComplete is a redis-like (BullMQ) option
      options,
    );
  });

  it("rejects unknown native properties on both providers", async () => {
    const redisOptions = { native: { notARealOption: 1 } };
    await redisQueue.send(
      "j",
      {},
      // @ts-expect-error unknown native option
      redisOptions,
    );

    const awsOptions = { native: { notARealOption: 1 } };
    await awsQueue.send(
      "j",
      {},
      // @ts-expect-error unknown native option
      awsOptions,
    );
  });
});

describe("worker native options", () => {
  it("types worker native options per provider", () => {
    redisQueue.worker("job", async () => {}, {
      concurrency: 10,
      native: { lockDuration: 60_000, maxStalledCount: 2 },
    });
    awsQueue.worker("job", async () => {}, {
      concurrency: 10,
      native: { waitTimeSeconds: 20, visibilityTimeout: 60, maxNumberOfMessages: 10 },
    });

    const wrongOptions = { native: { waitTimeSeconds: 20 } };
    redisQueue.worker(
      "job",
      async () => {},
      // @ts-expect-error waitTimeSeconds is SQS-specific
      wrongOptions,
    );
  });
});

describe("native client access", () => {
  it("returns the provider-specific client type", () => {
    expectTypeOf(redisQueue.native()).toEqualTypeOf<{ addJob(name: string): Promise<string> }>();
    expectTypeOf(awsQueue.native()).toEqualTypeOf<{ sendMessageBatch(): Promise<void> }>();
  });

  it("types send results through nativeResult", async () => {
    const result = await redisQueue.send("email.send", {});
    expectTypeOf(result.native).toEqualTypeOf<{ jobId: string }>();
    expectTypeOf(result.id).toEqualTypeOf<string>();
  });

  it("types native message handles on worker jobs", () => {
    redisQueue.worker(sendEmailJob, async (job) => {
      expectTypeOf(job.native).toEqualTypeOf<{ jobId: string }>();
      return { providerId: "x" };
    });
  });
});

describe("typed job definitions", () => {
  it("checks send payload types", async () => {
    await redisQueue.send(sendEmailJob, { to: "a@b.c", subject: "Hi" });

    await redisQueue.send(
      sendEmailJob,
      // @ts-expect-error payload is missing the required subject
      { to: "a@b.c" },
    );

    await redisQueue.send(
      sendEmailJob,
      // @ts-expect-error payload does not match the job input type
      { totally: "wrong" },
    );
  });

  it("infers job.data for handlers", () => {
    redisQueue.worker(sendEmailJob, async (job) => {
      expectTypeOf(job.data).toEqualTypeOf<{ to: string; subject: string }>();
      return { providerId: "x" };
    });

    redisQueue.worker(
      sendEmailJob,
      // @ts-expect-error handler must return the declared result type
      async (): Promise<string> => "not the result type",
    );
  });
});
