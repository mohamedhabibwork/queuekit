import { BaseQueueProvider } from "../core/base-provider.js";
import { jsonCodec } from "../core/codec.js";
import { resolveDelay } from "../core/delay.js";
import { createConsumer } from "../core/lifecycle.js";
import type { CloudflareConfig } from "../config.js";
import {
  QueueAuthenticationError,
  QueueAuthorizationError,
  QueueConfigError,
  QueueConnectionError,
  QueueConsumeError,
  QueuePublishError,
  QueueRateLimitError,
} from "../core/errors.js";
import type {
  ConsumerOptions,
  PublishOptions,
  PublishResult,
  QueueAcknowledgement,
  QueueCapabilities,
  QueueConsumer,
  QueueHandler,
  QueueMessage,
} from "../core/types.js";

const DEFAULT_API_BASE = "https://api.cloudflare.com/client/v4";
const REQUEST_TIMEOUT_MS = 30_000;
const RETRY_BACKOFF_MS = 1_000;
const PAUSE_TICK_MS = 20;
/** Pull is short polling; idle between empty batches instead of spinning on the API. */
const POLL_IDLE_MS = 250;

/** One message as the pull API returns it. */
export interface CloudflarePulledMessage {
  readonly id?: string;
  readonly body: string;
  readonly timestamp_ms?: number;
  readonly attempts?: number;
  readonly metadata?: Record<string, unknown>;
  /** Lease held while the message is in flight; pass it back to ack or retry. */
  readonly lease_id: string;
}

interface CloudflareResponse<TResult> {
  readonly success?: boolean;
  readonly errors?: readonly { readonly code?: number; readonly message?: string }[];
  readonly result?: TResult;
}

interface CloudflarePulledBatch {
  readonly messages?: readonly CloudflarePulledMessage[];
}

interface CloudflareEnvelope {
  readonly type?: string;
  readonly payload: unknown;
  readonly headers?: Record<string, unknown>;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly traceId?: string;
  readonly timestamp?: number;
}

export type CloudflareAcknowledgement = QueueAcknowledgement<CloudflarePulledMessage>;
/** Native push options: `content_type`/`delay_seconds` from the Queues API. */
export type CloudflarePublishOptions = {
  readonly contentType?: "text" | "json" | "bytes" | "v8";
  readonly delaySeconds?: number;
};
/** Native pull options: `visibility_timeout_ms`/`batch_size` from the pull API. */
export type CloudflareConsumerOptions = {
  readonly visibilityTimeoutMs?: number;
  readonly batchSize?: number;
};

export class CloudflareProvider extends BaseQueueProvider<"cloudflare"> {
  readonly name = "cloudflare" as const;
  readonly capabilities: QueueCapabilities = {
    kind: "queue",
    publish: true,
    consume: true,
    batchPublish: true,
    delayed: true,
    retries: true,
    ttl: false,
    deadLetter: true,
    ack: true,
    nack: true,
  };
  #controllers = new Set<AbortController>();

  constructor(readonly config: CloudflareConfig) {
    super();
  }

  #id(destination: string): string {
    return destination || this.config.queueId;
  }

  /** Workers bindings cover publish; everything else needs account + token. */
  #credentials(operation: "publish" | "publishMany" | "consume" | "ack"): {
    readonly accountId: string;
    readonly apiToken: string;
  } {
    const { accountId, apiToken } = this.config;
    if (!accountId || !apiToken) {
      throw new QueueConfigError(
        `Cloudflare ${operation} outside a Worker requires config.accountId and config.apiToken (publish can alternatively use a config.binding).`,
        { provider: this.name, operation },
      );
    }
    return { accountId, apiToken };
  }

  async #request<TResult>(
    operation: "publish" | "publishMany" | "consume" | "ack",
    queueId: string,
    path: string,
    body: unknown,
  ): Promise<CloudflareResponse<TResult>> {
    const { accountId, apiToken } = this.#credentials(operation);
    const base = (this.config.apiBaseUrl ?? DEFAULT_API_BASE).replace(/\/+$/, "");
    let response: Response;
    try {
      response = await fetch(`${base}/accounts/${accountId}/queues/${queueId}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${apiToken}` },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (cause) {
      throw new QueueConnectionError(
        `Cloudflare ${operation} request failed: ${String(cause)}`,
        { provider: this.name, operation, retryable: true },
        { cause },
      );
    }
    let payload: CloudflareResponse<TResult>;
    try {
      payload = (await response.json()) as CloudflareResponse<TResult>;
    } catch (cause) {
      throw new QueueConnectionError(
        `Cloudflare ${operation} returned a non-JSON response (HTTP ${response.status}).`,
        { provider: this.name, operation, statusCode: response.status, retryable: true },
        { cause },
      );
    }
    if (!response.ok || payload.success === false) {
      const message =
        payload.errors?.map((error) => error.message ?? String(error.code)).join("; ") ||
        `HTTP ${response.status}`;
      const context = {
        provider: this.name,
        operation,
        statusCode: response.status,
        native: payload.errors,
        retryable: response.status >= 500,
      };
      if (response.status === 401) throw new QueueAuthenticationError(message, context);
      if (response.status === 403) throw new QueueAuthorizationError(message, context);
      if (response.status === 429) throw new QueueRateLimitError(message, context);
      throw operation === "consume" || operation === "ack"
        ? new QueueConsumeError(message, context)
        : new QueuePublishError(message, context);
    }
    return payload;
  }

  static #envelope<TPayload>(message: QueueMessage<TPayload>): CloudflareEnvelope {
    return {
      type: message.type,
      payload: message.payload,
      headers: message.headers,
      correlationId: message.correlationId,
      causationId: message.causationId,
      traceId: message.traceId,
      timestamp: message.timestamp ?? Date.now(),
    };
  }

  static #delaySeconds(options: PublishOptions | undefined): number | undefined {
    const delay = resolveDelay(options);
    return delay === undefined ? undefined : Math.max(0, Math.floor(delay / 1000));
  }

  async #push(
    operation: "publish" | "publishMany",
    destination: string,
    envelopes: readonly CloudflareEnvelope[],
    options: PublishOptions<CloudflarePublishOptions> | undefined,
  ): Promise<PublishResult<"cloudflare", unknown>> {
    const binding = this.config.binding;
    if (binding) {
      const nativeOptions = {
        contentType: options?.native?.contentType ?? "json",
        delaySeconds: options?.native?.delaySeconds ?? CloudflareProvider.#delaySeconds(options),
      };
      if (envelopes.length === 1) {
        await binding.send(envelopes[0], nativeOptions);
      } else if (binding.sendBatch) {
        await binding.sendBatch(envelopes, nativeOptions);
      } else {
        await Promise.all(envelopes.map((envelope) => binding.send(envelope, nativeOptions)));
      }
      return { ok: true, provider: this.name, native: undefined };
    }
    const queueId = this.#id(destination);
    const body =
      operation === "publish"
        ? {
            body: envelopes[0],
            content_type: options?.native?.contentType ?? "json",
            delay_seconds:
              options?.native?.delaySeconds ?? CloudflareProvider.#delaySeconds(options),
          }
        : {
            messages: envelopes.map((envelope) => ({
              body: envelope,
              content_type: options?.native?.contentType ?? "json",
              delay_seconds:
                options?.native?.delaySeconds ?? CloudflareProvider.#delaySeconds(options),
            })),
          };
    const response = await this.#request<Record<string, unknown>>(
      operation,
      queueId,
      operation === "publish" ? "/messages" : "/messages/batch",
      body,
    );
    return { ok: true, provider: this.name, native: response.result };
  }

  async publish<TPayload>(
    destination: string,
    message: QueueMessage<TPayload>,
    options?: PublishOptions<CloudflarePublishOptions>,
  ): Promise<PublishResult<"cloudflare", unknown>> {
    let result: PublishResult<"cloudflare", unknown> | undefined;
    await this.operation(
      {
        provider: this.name,
        operation: "publish",
        destination: this.#id(destination),
        traceId: message.traceId,
        correlationId: message.correlationId,
        metadata: message.metadata,
      },
      async () => {
        result = await this.#push(
          "publish",
          destination,
          [CloudflareProvider.#envelope(message)],
          options,
        );
      },
    );
    return result!;
  }

  async publishMany<TPayload>(
    destination: string,
    messages: readonly QueueMessage<TPayload>[],
    options?: PublishOptions<CloudflarePublishOptions>,
  ): Promise<readonly PublishResult<"cloudflare", unknown>[]> {
    let result: readonly PublishResult<"cloudflare", unknown>[] | undefined;
    await this.operation(
      {
        provider: this.name,
        operation: "publishMany",
        destination: this.#id(destination),
        metadata: messages[0]?.metadata,
      },
      async () => {
        const envelopes = messages.map((message) => CloudflareProvider.#envelope(message));
        const response = await this.#push("publishMany", destination, envelopes, options);
        result = envelopes.map(() => response);
      },
    );
    return result!;
  }

  async consume<TPayload>(
    destination: string,
    handler: QueueHandler<TPayload, CloudflarePulledMessage, CloudflareAcknowledgement>,
    options?: ConsumerOptions<CloudflareConsumerOptions>,
  ): Promise<QueueConsumer> {
    this.#credentials("consume");
    const queueId = this.#id(destination);
    const controller = new AbortController();
    this.#controllers.add(controller);
    let paused = false;
    const pull = () =>
      this.#request<CloudflarePulledBatch>("consume", queueId, "/messages/pull", {
        visibility_timeout_ms:
          options?.native?.visibilityTimeoutMs ?? this.config.visibilityTimeoutMs,
        batch_size: options?.native?.batchSize ?? this.config.batchSize,
      });
    const settle = (action: "acks" | "retries", entries: unknown[]) =>
      this.#request("ack", queueId, "/messages/ack", {
        acks: action === "acks" ? entries : [],
        retries: action === "retries" ? entries : [],
      });
    let handle: QueueConsumer | undefined;
    const run = async () => {
      while (!controller.signal.aborted) {
        if (paused) {
          await new Promise((resolve) => setTimeout(resolve, PAUSE_TICK_MS));
          continue;
        }
        let batch: readonly CloudflarePulledMessage[];
        try {
          batch = (await pull()).result?.messages ?? [];
        } catch (error) {
          if (controller.signal.aborted) return;
          // Transient pull failures back off and retry; auth failures are fatal
          // and stop the consumer (the loop would otherwise spin on 401s).
          if (
            error instanceof QueueAuthenticationError ||
            error instanceof QueueAuthorizationError
          ) {
            await handle?.close();
            return;
          }
          await new Promise((resolve) => setTimeout(resolve, RETRY_BACKOFF_MS));
          continue;
        }
        if (batch.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, POLL_IDLE_MS));
          continue;
        }
        for (const raw of batch) {
          if (controller.signal.aborted) return;
          let settled = false;
          const completeLease = () =>
            settle("acks", [{ lease_id: raw.lease_id }]).then(() => undefined);
          const retryLease = (delayMs: number | undefined) =>
            settle("retries", [
              {
                lease_id: raw.lease_id,
                delay_seconds:
                  delayMs === undefined ? undefined : Math.max(0, Math.floor(delayMs / 1000)),
              },
            ]).then(() => undefined);
          const ack: CloudflareAcknowledgement = {
            native: raw,
            complete: async () => {
              if (!settled) {
                await completeLease();
                settled = true;
              }
            },
            retry: async (retry) => {
              if (!settled) {
                await retryLease(retry?.delay);
                settled = true;
              }
            },
            reject: async (reject) => {
              if (!settled) {
                // No dead-letter endpoint on the pull API: requeue retries
                // immediately, non-requeue drops the lease by acknowledging.
                if (reject?.requeue === false) await completeLease();
                else await retryLease(0);
                settled = true;
              }
            },
          };
          let envelope: CloudflareEnvelope;
          try {
            envelope = jsonCodec.decode(raw.body) as CloudflareEnvelope;
          } catch {
            // Not a queuekit envelope (or not JSON): give the lease back immediately.
            if (!settled) {
              await retryLease(0);
              settled = true;
            }
            continue;
          }
          try {
            await handler({
              message: {
                id: raw.id,
                type: envelope.type,
                payload: envelope.payload as TPayload,
                headers: envelope.headers ?? {},
                attempt: raw.attempts,
                timestamp: envelope.timestamp ?? raw.timestamp_ms,
                correlationId: envelope.correlationId,
                causationId: envelope.causationId,
                traceId: envelope.traceId,
                native: raw,
              },
              ack,
              signal: controller.signal,
            });
            if (options?.autoAck !== false) await ack.complete();
          } catch {
            if (options?.autoAck !== false && !settled) {
              await retryLease(undefined);
              settled = true;
            }
          }
        }
      }
    };
    void run();
    const consumer = createConsumer({
      pause: async () => {
        paused = true;
      },
      resume: async () => {
        paused = false;
      },
      close: async () => {
        controller.abort();
        this.#controllers.delete(controller);
      },
    });
    handle = consumer;
    return consumer;
  }

  native(): CloudflareConfig["binding"] {
    return this.config.binding;
  }

  async close(): Promise<void> {
    this.markClosed();
    for (const controller of this.#controllers) controller.abort();
    this.#controllers.clear();
  }
}

export async function createCloudflare(config: CloudflareConfig): Promise<CloudflareProvider> {
  return new CloudflareProvider(config);
}
