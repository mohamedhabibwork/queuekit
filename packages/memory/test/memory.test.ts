import { describe, expect, it, vi } from "vitest";
import { createQueue, defineJob, FakeClock, QueueTimeoutError, type QueueWorker } from "@mohamedhabibwork/core";
import { memory } from "../src/index";
import { captureJobs, waitForQueueEvent, waitForWorkerEvent } from "@mohamedhabibwork/core/testing";

function setup(name = "emails") {
  const clock = new FakeClock();
  const provider = memory({ clock, defaultVisibilityTimeout: "30s" });
  const queue = createQueue({ name, provider, clock });
  return { clock, provider, queue };
}

/** Real-clock setup for tests that exercise wall-clock behaviour. */
function setupReal(name: string) {
  const provider = memory();
  const queue = createQueue({ name, provider });
  return { queue };
}

async function settle(times = 5): Promise<void> {
  for (let i = 0; i < times; i++) {
    await Promise.resolve();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }
}

describe("memory provider — send/receive", () => {
  it("delivers a typed payload to a worker", async () => {
    const { queue } = setup();
    const handler = vi.fn(async (job: { data: { to: string } }) => {
      expect(job.data.to).toBe("user@example.com");
    });
    const sendEmail = defineJob<{ to: string }, undefined>("email.send");
    const worker = queue.worker(sendEmail, handler, { pollInterval: "10ms" });

    await queue.send(sendEmail, { to: "user@example.com" });
    await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    await worker.close();
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("returns handler results through the completed event", async () => {
    const { queue } = setup("tasks");
    const sendEmail = defineJob<{ to: string; subject: string }, string>("email.send");
    const worker = queue.worker(sendEmail, async (job) => `sent:${job.data.subject}`);

    await queue.send(sendEmail, { to: "x@y.z", subject: "Hello" });
    const event = await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    expect(event.result).toBe("sent:Hello");
    await worker.close();
  });

  it("supports plain string job names", async () => {
    const { queue } = setupReal("cleanup");
    const worker = queue.worker("cleanup", async () => "done", { pollInterval: "10ms" });
    await queue.send("cleanup", {});
    const event = await waitForWorkerEvent(worker, "completed", { timeout: "3s" });
    expect(event.result).toBe("done");
    await worker.close();
  });
});

describe("memory provider — delay and scheduling", () => {
  it("holds delayed messages until the clock reaches them", async () => {
    const { clock, queue } = setup();
    const processed: string[] = [];
    const worker = queue.worker(
      "late",
      async (job: { data: unknown }) => {
        processed.push(job.data as string);
      },
      { pollInterval: "10ms" },
    );

    await queue.send("late", "immediate");
    await queue.send("late", "delayed", { delay: "5m" });

    await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    await settle();
    expect(processed).toEqual(["immediate"]);

    clock.advance("5m");
    await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    await settle();
    expect(processed).toEqual(["immediate", "delayed"]);
    await worker.close();
  });

  it("schedules at an absolute time via delayed delivery", async () => {
    const { clock, queue } = setup("scheduled");
    const seen = waitForQueueEvent(queue, "sent", { timeout: "1s" });
    const result = await queue.schedule("reminder", {}, { at: new Date(clock.now() + 60_000) });
    expect(result.strategy).toBe("delayed-delivery");
    await seen;
  });
});

describe("memory provider — retries and dead letters", () => {
  it("retries failures with backoff until attempts are exhausted", async () => {
    const { clock, queue } = setup("flaky");
    const attempts: number[] = [];
    const worker = queue.worker(
      "flaky.job",
      async (job: { attempt: number }) => {
        attempts.push(job.attempt);
        throw new Error("still broken");
      },
      {
        pollInterval: "10ms",
        retry: { attempts: 3, backoff: { strategy: "fixed", delay: "1m" } },
      },
    );

    await queue.send("flaky.job", {});
    await waitForWorkerEvent(worker, "retrying", { timeout: "2s", filter: (e) => e.nextAttempt === 2 });
    expect(attempts).toEqual([1]);

    clock.advance("1m");
    await waitForWorkerEvent(worker, "retrying", { timeout: "2s", filter: (e) => e.nextAttempt === 3 });
    expect(attempts).toEqual([1, 2]);

    clock.advance("1m");
    await waitForWorkerEvent(worker, "failed", { timeout: "2s", filter: (e) => !e.willRetry });
    await settle();
    expect(attempts).toEqual([1, 2, 3]);
    await worker.close();
  });

  it("dead-letters exhausted messages into the configured queue", async () => {
    const { queue } = setup("billing");
    const worker = queue.worker(
      "charge",
      async () => {
        throw new Error("card declined");
      },
      {
        pollInterval: "10ms",
        retry: { attempts: 2, backoff: { strategy: "fixed", delay: 0 } },
        deadLetter: { queue: "billing.dlq" },
      },
    );

    await queue.send("charge", { invoice: "A1" });
    const dead = await waitForWorkerEvent(worker, "dead-lettered", { timeout: "2s" });
    expect(dead.deadLetterQueue).toBe("billing.dlq");
    expect(dead.error).toBeInstanceOf(Error);
    await worker.close();
  });

  it("skips retries for fatal errors", async () => {
    const { queue } = setup("fatal");
    const attempts: number[] = [];
    const worker = queue.worker(
      "fatal.job",
      async (job: { attempt: number }) => {
        attempts.push(job.attempt);
        const error = new Error("permanent");
        (error as { retryable?: unknown }).retryable = false;
        throw error;
      },
      { pollInterval: "10ms", retry: { attempts: 5 } },
    );
    await queue.send("fatal.job", {});
    await waitForWorkerEvent(worker, "failed", { timeout: "2s", filter: (e) => !e.willRetry });
    await settle();
    expect(attempts).toEqual([1]);
    await worker.close();
  });

  it("respects retry.when predicates", async () => {
    const { queue } = setup("when");
    const attempts: number[] = [];
    const worker = queue.worker(
      "when.job",
      async (job: { attempt: number }) => {
        attempts.push(job.attempt);
        throw new Error("no retry for this");
      },
      { pollInterval: "10ms", retry: { attempts: 5, when: () => false } },
    );
    await queue.send("when.job", {});
    await waitForWorkerEvent(worker, "failed", { timeout: "2s", filter: (e) => !e.willRetry });
    await settle();
    expect(attempts).toEqual([1]);
    await worker.close();
  });
});

describe("memory provider — validation", () => {
  const schema = {
    "~standard": {
      version: 1 as const,
      vendor: "test",
      validate: (value: unknown) =>
        typeof value === "object" && value !== null && "to" in value
          ? { value }
          : { issues: [{ message: "missing 'to'" }] },
    },
  };

  it("validates producer payloads and rejects invalid ones", async () => {
    const { queue } = setup("validated");
    const job = defineJob({ name: "email.send", input: schema });
    await expect(queue.send(job, { nope: 1 })).rejects.toThrow(/failed validation/);
  });

  it("validates consumer payloads without retry loops", async () => {
    const { queue } = setup("consumer");
    const job = defineJob({ name: "guarded", input: schema });
    const handler = vi.fn(async () => {});
    const worker = queue.worker(job, handler, { pollInterval: "10ms", retry: { attempts: 5 } });

    await queue.send("guarded", { malformed: true } as never);
    await waitForWorkerEvent(worker, "failed", { timeout: "2s", filter: (e) => !e.willRetry });
    await settle(10);
    expect(handler).not.toHaveBeenCalled();
    await worker.close();
  });

  it("dead-letters poison messages that cannot be decoded", async () => {
    const clock = new FakeClock();
    const broken = createQueue({
      name: "poison",
      provider: memory({ clock }),
      serializer: {
        encode: (value) => JSON.stringify(value),
        decode: () => {
          throw new Error("not JSON");
        },
      },
    });
    const worker = broken.worker("anything", async () => {}, {
      pollInterval: "10ms",
      deadLetter: { queue: "poison.dlq" },
    });
    await broken.send("anything", {});
    const dead = await waitForWorkerEvent(worker, "dead-lettered", { timeout: "2s" });
    expect(dead.deadLetterQueue).toBe("poison.dlq");
    await worker.close();
  });
});

describe("memory provider — worker lifecycle", () => {
  it("processes messages concurrently", async () => {
    const { queue } = setup("concurrent");
    let active = 0;
    let maxActive = 0;
    const worker = queue.worker(
      "work",
      async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 20));
        active -= 1;
      },
      { concurrency: 4, pollInterval: "5ms" },
    );

    await Promise.all([queue.send("work", 1), queue.send("work", 2), queue.send("work", 3), queue.send("work", 4)]);
    await settle(40);
    expect(maxActive).toBeGreaterThan(1);
    await worker.close();
  });

  it("does not start when autoStart is false until start() is called", async () => {
    const { queue } = setup("manual");
    const processed: number[] = [];
    const worker = queue.worker(
      "tick",
      async (job: { data: unknown }) => {
        processed.push(job.data as number);
      },
      { autoStart: false, pollInterval: "10ms" },
    );
    expect(worker.running).toBe(false);

    await queue.send("tick", 1);
    await settle(10);
    expect(processed).toEqual([]);

    worker.start();
    expect(worker.running).toBe(true);
    await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    expect(processed).toEqual([1]);
    await worker.close();
  });

  it("pauses and resumes", async () => {
    const { queue } = setup("pausable");
    const processed: number[] = [];
    const worker = queue.worker(
      "tick",
      async (job: { data: unknown }) => {
        processed.push(job.data as number);
      },
      { pollInterval: "10ms" },
    );
    worker.pause();
    expect(worker.paused).toBe(true);

    await queue.send("tick", 1);
    await settle(10);
    expect(processed).toEqual([]);

    worker.resume();
    expect(worker.paused).toBe(false);
    await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    expect(processed).toEqual([1]);
    await worker.close();
  });

  it("stops fetching on close and lets active jobs finish", async () => {
    const { queue } = setup("shutdown");
    let finished = false;
    const worker = queue.worker(
      "slow",
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 30));
        finished = true;
      },
      { pollInterval: "10ms" },
    );

    await queue.send("slow", {});
    await waitForWorkerEvent(worker, "received", { timeout: "2s" });
    await worker.close({ timeout: "5s" });
    expect(finished).toBe(true);
    expect(worker.running).toBe(false);
  });

  it("times out handlers exceeding the processing timeout (real clock)", async () => {
    const { queue } = setupReal("timeouts");
    const worker = queue.worker(
      "hangs",
      async () => {
        await new Promise(() => {});
      },
      { pollInterval: "10ms", timeout: "50ms", retry: { attempts: 1 } },
    );
    await queue.send("hangs", {});
    const failed = await waitForWorkerEvent(worker, "failed", { timeout: "3s" });
    expect(failed.error).toBeInstanceOf(QueueTimeoutError);
    // A force close aborts the hanging handler for real.
    await worker.close({ force: true, timeout: "1s" });
  });
});

describe("memory provider — envelope fidelity", () => {
  it("roundtrips metadata and exposes correlation ids", async () => {
    const { queue } = setup("meta");
    const seen: Array<Record<string, string>> = [];
    const worker = queue.worker(
      "meta.job",
      async (job: { metadata: Record<string, string> }) => {
        seen.push(job.metadata);
      },
      { pollInterval: "10ms" },
    );
    await queue.send("meta.job", {}, { metadata: { tenant: "acme" }, correlationId: "req-42" });
    await waitForWorkerEvent(worker, "completed", { timeout: "2s" });
    expect(seen[0]).toMatchObject({ tenant: "acme", "correlation-id": "req-42" });
    await worker.close();
  });

  it("deduplicates sends carrying an idempotency key", async () => {
    const { queue } = setup("dedup");
    const first = await queue.send("payment", { id: "p1" }, { idempotencyKey: "payment:p1" });
    const second = await queue.send("payment", { id: "p1" }, { idempotencyKey: "payment:p1" });
    expect(first.deduplication.mode).toBe("none");
    expect(second.deduplication.mode).toBe("adapter");
    expect(first.id).toBe(second.id);
  });

  it("batches sends and reports native results", async () => {
    const { queue } = setup("batched");
    const result = await queue.sendBatch([
      { job: "a", data: 1 },
      { job: "b", data: 2 },
    ]);
    expect(result.successful).toHaveLength(2);
    expect(result.successful[0]?.native.deduplicated).toBe(false);
  });

  it("supports priorities: higher first", async () => {
    const { queue } = setup("priority");
    const order: number[] = [];
    const worker = queue.worker(
      "p.job",
      async (job: { data: unknown }) => {
        order.push(job.data as number);
      },
      { concurrency: 1, pollInterval: "10ms" },
    );
    worker.pause();

    await queue.send("p.job", 1, { native: { priority: 1 } });
    await queue.send("p.job", 10, { native: { priority: 10 } });
    await queue.send("p.job", 5, { native: { priority: 5 } });

    worker.resume();
    await settle(40);
    expect(order).toEqual([10, 5, 1]);
    await worker.close();
  });

  it("captures sends through testing helpers", async () => {
    const { queue } = setup("capture");
    const capture = captureJobs(queue);
    await queue.send("email.send", { to: "a@b.c" });
    expect(capture.jobs).toHaveLength(1);
    expect(capture.jobs[0]?.name).toBe("email.send");
    expect(capture.jobs[0]?.payload).toEqual({ to: "a@b.c" });
  });
});

describe("memory provider — operations", () => {
  it("reports health and queue size", async () => {
    const { queue } = setup("ops");
    await queue.send("job", {}, { delay: "1m" });
    await queue.send("job", {});
    const size = await queue.size();
    expect(size.total).toBe(2);
    expect(size.pending).toBe(1);
    expect(size.delayed).toBe(1);

    const health = await queue.health();
    expect(health.status).toBe("healthy");
  });

  it("purges the queue", async () => {
    const { queue } = setup("purge");
    await queue.send("job", {});
    await queue.purge();
    expect((await queue.size()).total).toBe(0);
  });

  it("exposes the typed memory inspector via native()", async () => {
    const { queue } = setup("inspect");
    await queue.send("job", {});
    const inspector = queue.native();
    expect(inspector.envelopeIds("inspect")).toHaveLength(1);
    const snapshot = inspector.snapshot("inspect");
    expect(snapshot[0]?.state).toBe("pending");
  });
});

describe("memory provider — visibility redelivery", () => {
  it("redelivers messages whose visibility lease expired before ack", { timeout: 20_000 }, async () => {
    const { clock, queue } = setup("leases");
    const attempts: number[] = [];
    const worker = queue.worker(
      "lease.job",
      async (job: { attempt: number }) => {
        attempts.push(job.attempt);
        // First delivery: hang forever (simulates a crash before ack).
        if (job.attempt === 1) {
          await new Promise(() => {});
        }
      },
      { pollInterval: "10ms", visibilityTimeout: "30s", retry: { attempts: 3 } },
    );

    await queue.send("lease.job", {});
    await waitForWorkerEvent(worker, "received", { timeout: "2s" });
    // Force close also stops the automatic visibility extension.
    await worker.close({ force: true, timeout: "1s" });
    expect(attempts).toEqual([1]);

    const secondWorker: QueueWorker = queue.worker("lease.job", async () => {}, {
      pollInterval: "10ms",
      visibilityTimeout: "30s",
    });
    clock.advance("31s");
    await waitForWorkerEvent(secondWorker, "received", { timeout: "2s" });
    await secondWorker.close();
  });
});
