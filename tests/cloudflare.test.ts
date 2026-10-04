import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  QueueAuthenticationError,
  QueueClosedError,
  QueueConfigError,
  QueuePublishError,
  QueueRateLimitError,
} from "../src/core/errors.js";
import type { CloudflareProvider } from "../src/drivers/cloudflare.js";
import { createQueue } from "../src/index.js";

/**
 * The Cloudflare provider is pure `fetch` against the Queues REST API, so the
 * whole contract (push, batch push, pull, ack/retry) is testable with a stubbed
 * global fetch — no account, no Workers runtime.
 */

interface RecordedCall {
  readonly url: string;
  readonly auth: string | undefined;
  readonly body: Record<string, unknown>;
}

const BASE = "https://api.cloudflare.test/client/v4";
const ACCOUNT = "acc-123";
const TOKEN = "tok-456";
const QUEUE = "queue-789";

function restConfig() {
  return {
    type: "cloudflare",
    queueId: QUEUE,
    accountId: ACCOUNT,
    apiToken: TOKEN,
    apiBaseUrl: BASE,
  } as const;
}

function cfResult(result: unknown, success = true, status = 200): Response {
  const payload = { success, errors: [], messages: [], result };
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function cfFailure(status: number, message: string): Response {
  return new Response(
    JSON.stringify({ success: false, errors: [{ code: 10_000, message }], messages: [] }),
    { status, headers: { "content-type": "application/json" } },
  );
}

/** Message the pull API returns for a queuekit envelope. */
function pulled(lease: string, envelopeBody: unknown, attempts = 1) {
  return {
    id: `m-${lease}`,
    body: JSON.stringify(envelopeBody),
    attempts,
    timestamp_ms: 1_690_000_000_000,
    lease_id: lease,
    metadata: { "CF-Content-Type": "json" },
  };
}

/** Poll until `predicate` holds (the consume loop runs in the background). */
async function until(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition not met before timeout");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe("cloudflare provider (REST)", () => {
  let calls: RecordedCall[];
  let provider: CloudflareProvider;
  /** Queued /messages/pull results; exhausted polls return an empty batch. */
  let pendingPulls: unknown[][];

  function stubFetch(response?: (url: string, body: Record<string, unknown>) => Response) {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        const body =
          typeof init?.body === "string" ? (JSON.parse(init.body) as Record<string, unknown>) : {};
        const auth = init?.headers
          ? (init.headers as Record<string, string>).authorization
          : undefined;
        calls.push({ url, auth, body });
        if (response) return response(url, body);
        if (url.endsWith("/messages/pull")) {
          return cfResult({ messages: pendingPulls.length > 0 ? pendingPulls.shift() : [] });
        }
        if (url.endsWith("/messages/ack")) return cfResult({ ackCount: 1, retryCount: 0 });
        return cfResult({});
      }),
    );
  }

  beforeEach(() => {
    calls = [];
    pendingPulls = [];
    stubFetch();
  });

  afterEach(async () => {
    await provider?.close().catch(() => undefined);
    vi.unstubAllGlobals();
  });

  const ackCalls = () => calls.filter((call) => call.url.endsWith("/messages/ack"));

  it("pushes the message envelope to /messages with bearer auth", async () => {
    provider = await createQueue(restConfig());
    const result = await provider.publish("orders", {
      type: "created",
      payload: { orderId: "o1" },
      correlationId: "c-1",
    });
    expect(result).toMatchObject({ ok: true, provider: "cloudflare" });
    expect(calls).toHaveLength(1);
    const call = calls[0]!;
    expect(call.url).toBe(`${BASE}/accounts/${ACCOUNT}/queues/orders/messages`);
    expect(call.auth).toBe(`Bearer ${TOKEN}`);
    expect(call.body).toEqual({
      body: {
        type: "created",
        payload: { orderId: "o1" },
        correlationId: "c-1",
        timestamp: expect.any(Number),
      },
      content_type: "json",
      delay_seconds: undefined,
    });
  });

  it("falls back to config.queueId and maps PublishOptions.delay", async () => {
    provider = await createQueue(restConfig());
    await provider.publish("", { payload: 1 }, { delay: 2_500 });
    expect(calls[0]!.url).toBe(`${BASE}/accounts/${ACCOUNT}/queues/${QUEUE}/messages`);
    expect(calls[0]!.body.delay_seconds).toBe(2);
  });

  it("publishes many through /messages/batch", async () => {
    provider = await createQueue(restConfig());
    const results = await provider.publishMany("q", [{ payload: "a" }, { payload: "b" }]);
    expect(results).toHaveLength(2);
    expect(calls[0]!.url).toBe(`${BASE}/accounts/${ACCOUNT}/queues/q/messages/batch`);
    expect((calls[0]!.body.messages as unknown[]).length).toBe(2);
  });

  it("requires account id and api token when no binding is set", async () => {
    provider = await createQueue({ type: "cloudflare", queueId: QUEUE });
    await expect(provider.publish("q", { payload: 1 })).rejects.toThrow(QueueConfigError);
    expect(calls).toHaveLength(0);
  });

  it("publishes through the Workers binding when one is configured", async () => {
    const binding = { send: vi.fn(async () => undefined), sendBatch: vi.fn(async () => undefined) };
    provider = await createQueue({ type: "cloudflare", queueId: QUEUE, binding });
    await provider.publish("q", { type: "ping", payload: 1 }, { delay: 1_000 });
    expect(binding.send).toHaveBeenCalledWith(
      { type: "ping", payload: 1, timestamp: expect.any(Number) },
      { contentType: "json", delaySeconds: 1 },
    );
    expect(calls).toHaveLength(0); // binding mode never hits the REST API
  });

  it("uses binding.sendBatch for publishMany and falls back to send", async () => {
    const withBatch = {
      send: vi.fn(async (_message: unknown, _options?: unknown) => undefined),
      sendBatch: vi.fn(async (_messages: readonly unknown[], _options?: unknown) => undefined),
    };
    provider = await createQueue({ type: "cloudflare", queueId: QUEUE, binding: withBatch });
    await provider.publishMany("q", [{ payload: 1 }, { payload: 2 }]);
    expect(withBatch.sendBatch).toHaveBeenCalledTimes(1);
    expect(withBatch.sendBatch.mock.calls[0]![0]).toHaveLength(2);

    const withoutBatch = {
      send: vi.fn(async (_message: unknown, _options?: unknown) => undefined),
    };
    await provider.close();
    provider = await createQueue({ type: "cloudflare", queueId: QUEUE, binding: withoutBatch });
    await provider.publishMany("q", [{ payload: 1 }, { payload: 2 }]);
    expect(withoutBatch.send).toHaveBeenCalledTimes(2);
  });

  it("maps 401, 429 and success:false responses to queue errors", async () => {
    provider = await createQueue(restConfig());
    stubFetch(() => cfFailure(401, "Authentication failed"));
    await expect(provider.publish("q", { payload: 1 })).rejects.toThrow(QueueAuthenticationError);

    stubFetch(() => cfFailure(429, "Rate limited"));
    await expect(provider.publish("q", { payload: 1 })).rejects.toThrow(QueueRateLimitError);

    stubFetch(
      () =>
        new Response(JSON.stringify({ success: false, errors: [{ code: 7, message: "nope" }] }), {
          status: 200,
        }),
    );
    await expect(provider.publish("q", { payload: 1 })).rejects.toThrow(QueuePublishError);
  });

  it("pulls messages, decodes the envelope and auto-acks by lease", async () => {
    provider = await createQueue(restConfig());
    pendingPulls = [[pulled("lease-1", { type: "hi", payload: { n: 1 } })]];
    const seen: unknown[] = [];
    const consumer = await provider.consume<{ n: number }>("q", ({ message }) => {
      seen.push(message.payload);
    });
    await until(() => ackCalls().length > 0);
    expect(seen).toEqual([{ n: 1 }]);
    expect(ackCalls()[0]!.body).toEqual({ acks: [{ lease_id: "lease-1" }], retries: [] });
    await consumer.close();
  });

  it("retries immediately when the handler throws and honors delay", async () => {
    provider = await createQueue(restConfig());
    pendingPulls = [[pulled("lease-2", { payload: "boom" })]];
    const consumer = await provider.consume("q", () => {
      throw new Error("handler exploded");
    });
    await until(() => ackCalls().some((call) => (call.body.retries as unknown[]).length > 0));
    const retry = ackCalls().find((call) => (call.body.retries as unknown[]).length > 0)!;
    expect(retry.body.acks).toEqual([]);
    expect(retry.body.retries).toEqual([{ lease_id: "lease-2", delay_seconds: undefined }]);
    await consumer.close();
  });

  it("supports manual ack.retry({ delay }) from the handler", async () => {
    provider = await createQueue(restConfig());
    pendingPulls = [[pulled("lease-3", { payload: "later" })]];
    const consumer = await provider.consume("q", async ({ ack }) => {
      await ack.retry!({ delay: 5_000 });
    });
    await until(() => ackCalls().some((call) => (call.body.retries as unknown[]).length > 0));
    expect(ackCalls()[0]!.body.retries).toEqual([{ lease_id: "lease-3", delay_seconds: 5 }]);
    await consumer.close();
  });

  it("drops the message on reject({ requeue: false }) and keeps no settle with autoAck off", async () => {
    provider = await createQueue(restConfig());
    pendingPulls = [
      [pulled("lease-4", { payload: "drop-me" })],
      [pulled("lease-5", { payload: "manual" })],
    ];
    const consumer = await provider.consume(
      "q",
      async (context) => {
        if (context.message.payload === "drop-me") {
          await context.ack.reject!({ requeue: false });
        }
        // second message: autoAck false leaves the lease untouched
      },
      { autoAck: false },
    );
    await until(() => calls.filter((call) => call.url.endsWith("/messages/pull")).length >= 3);
    expect(ackCalls()[0]!.body.acks).toEqual([{ lease_id: "lease-4" }]);
    expect(ackCalls()).toHaveLength(1); // lease-5 was never settled
    await consumer.close();
  });

  it("requeues malformed envelopes without invoking the handler", async () => {
    provider = await createQueue(restConfig());
    pendingPulls = [
      [
        {
          id: "m-x",
          body: "definitely not json {",
          attempts: 3,
          lease_id: "lease-6",
        },
      ],
    ];
    let handled = 0;
    const consumer = await provider.consume("q", () => {
      handled += 1;
    });
    await until(() => ackCalls().some((call) => (call.body.retries as unknown[]).length > 0));
    expect(handled).toBe(0);
    expect(ackCalls()[0]!.body.retries).toEqual([{ lease_id: "lease-6", delay_seconds: 0 }]);
    await consumer.close();
  });

  it("closes the consumer when pulls keep returning 401", async () => {
    provider = await createQueue(restConfig());
    stubFetch(() => cfFailure(401, "Token expired"));
    const consumer = await provider.consume("q", () => undefined);
    await until(() => consumer.status === "closed");
  });

  it("rejects publish after close", async () => {
    provider = await createQueue(restConfig());
    await provider.close();
    await expect(provider.publish("q", { payload: 1 })).rejects.toThrow(QueueClosedError);
  });
});
