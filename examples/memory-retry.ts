/**
 * Retries, backoff, dead-lettering and the FakeClock — the deterministic
 * failure-path story of Queue Kit.
 *
 * Run with: bun examples/memory-retry.ts
 */
import { createQueue, defineJob, FakeClock } from "@queue-kit/core";
import { memory } from "@queue-kit/memory";

const clock = new FakeClock();
const queue = createQueue({ name: "payments", provider: memory({ clock }) });

const chargeCard = defineJob<{ invoiceId: string }, void>("payment.charge");

let attempts = 0;

const worker = queue.worker(
  chargeCard,
  async (job) => {
    attempts += 1;
    console.log(`attempt ${job.attempt} for ${job.data.invoiceId}`);
    if (job.attempt < 3) {
      throw new Error("gateway timeout"); // retryable by default
    }
    console.log("charged successfully on the third attempt");
  },
  {
    pollInterval: "10ms",
    retry: {
      attempts: 5,
      backoff: { strategy: "exponential", delay: "1m" },
    },
    deadLetter: { queue: "payments.dlq" }, // poison work lands here, inspectable
  },
);

await queue.send(chargeCard, { invoiceId: "INV-42" });
const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
await wait(30); // let the worker pick up attempt 1 and fail

// Deterministic: no real sleeping. Advancing the fake clock releases the
// backoff window exactly like real time would.
clock.advance("1m"); // -> attempt 2
await wait(30);
clock.advance("2m"); // -> attempt 3 (succeeds)
await wait(30);
console.log(`finished after ${attempts} attempts`);
await worker.close();
await queue.close();
