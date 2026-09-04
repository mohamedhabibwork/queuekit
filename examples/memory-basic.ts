/**
 * Basic Queue Kit example: send and consume jobs on the in-memory provider.
 *
 * Run with:
 *   bun examples/memory-basic.ts
 *   node --experimental-strip-types examples/memory-basic.ts
 *   deno run -A examples/memory-basic.ts   (with node_modules installed)
 */
import { createQueue, defineJob, type QueueJob } from "@queue-kit/core";
import { memory } from "@queue-kit/memory";

// 1. Define a reusable, typed job — the contract between producer and worker.
interface EmailPayload {
  to: string;
  subject: string;
}

const sendEmail = defineJob<EmailPayload, { messageId: string }>("email.send");

// 2. Create a queue on a provider. Swap `memory()` for `bullmq(...)` or
//    `sqs(...)` and nothing else in this file needs to change.
const queue = createQueue({ name: "emails", provider: memory() });

// 3. Produce — payload types flow from the job definition.
await queue.send(sendEmail, { to: "ada@example.com", subject: "Welcome" });
await queue.send(sendEmail, { to: "grace@example.com", subject: "Welcome" }, {
  delay: "2s", // portable: earliest time before which the provider delivers
});

// 4. Consume — job.data is fully inferred, native handle stays typed.
const worker = queue.worker(
  sendEmail,
  async (job: QueueJob<EmailPayload, unknown>) => {
    console.log(`sending "${job.data.subject}" to ${job.data.to} (attempt ${job.attempt})`);
    return { messageId: `smtp-${job.id.slice(0, 8)}` };
  },
  { concurrency: 4, pollInterval: "50ms" },
);

// 5. Observe.
worker.on("completed", (event) => {
  console.log(`✔ ${event.jobName} finished in ${event.durationMs}ms`);
});

// 6. Graceful shutdown: stop fetching, let active jobs finish, release resources.
await new Promise((resolve) => setTimeout(resolve, 500));
await worker.close({ timeout: "5s" });
await queue.close();
console.log("done");
