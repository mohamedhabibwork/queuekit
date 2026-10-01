import { computeBackoff } from "./backoff.js";
import { QueueTimeoutError } from "./errors.js";
import type { MessageContext, QueueAcknowledgement, RetryPolicy } from "./types.js";

export type ContextHandler<TPayload, TNative, TAck extends QueueAcknowledgement> = (
  context: MessageContext<TPayload, TNative, TAck>,
) => Promise<void> | void;

export interface WithRetryOptions extends RetryPolicy {
  /** Called after each failed attempt, before retrying or dead-lettering. */
  readonly onFailure?: (error: unknown, attempt: number) => void | Promise<void>;
}

const DEFAULT_ATTEMPTS = 3;

/**
 * Wraps a handler with provider-agnostic retries. A failing attempt is sent back
 * via `ack.retry({ delay })` using the backoff policy; once `attempts` is reached
 * the message is rejected without requeue (dead-lettered where supported).
 * Providers without `retry`/`reject` see the original error rethrown.
 */
export function withRetry<TPayload, TNative, TAck extends QueueAcknowledgement>(
  handler: ContextHandler<TPayload, TNative, TAck>,
  options: WithRetryOptions = {},
): ContextHandler<TPayload, TNative, TAck> {
  const attempts = Math.max(1, options.attempts ?? DEFAULT_ATTEMPTS);
  return async (context) => {
    try {
      await handler(context);
    } catch (error) {
      const attempt = context.message.attempt ?? 1;
      await options.onFailure?.(error, attempt);
      const { ack } = context;
      if (attempt < attempts && ack.retry) {
        await ack.retry({ delay: computeBackoff(options, attempt) });
        return;
      }
      if (attempt >= attempts && ack.reject) {
        await ack.reject({ requeue: false });
        return;
      }
      throw error;
    }
  };
}

/**
 * Fails the handler with `QueueTimeoutError` after `milliseconds` and aborts the
 * signal passed to it, so cooperative work (fetch, db calls) can stop early.
 */
export function withTimeout<TPayload, TNative, TAck extends QueueAcknowledgement>(
  handler: ContextHandler<TPayload, TNative, TAck>,
  milliseconds: number,
): ContextHandler<TPayload, TNative, TAck> {
  return async (context) => {
    const controller = new AbortController();
    const forward = () => controller.abort(context.signal.reason);
    if (context.signal.aborted) forward();
    else context.signal.addEventListener("abort", forward, { once: true });
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        const error = new QueueTimeoutError(`Handler exceeded ${milliseconds}ms.`, {
          operation: "consume",
          retryable: true,
        });
        controller.abort(error);
        reject(error);
      }, milliseconds);
    });
    try {
      await Promise.race([
        Promise.resolve(handler({ ...context, signal: controller.signal })),
        timeout,
      ]);
    } finally {
      clearTimeout(timer);
      context.signal.removeEventListener("abort", forward);
    }
  };
}
