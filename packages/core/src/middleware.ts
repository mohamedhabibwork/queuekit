import type { QueueEnvelope } from "./envelope";
import type { QueueCapabilities } from "./capabilities";
import type { ProviderTypes, QueueRef } from "./types/provider";
import type { SendResult } from "./types/results";
import type { MutableSendOptions } from "./types/options";
import type { QueueJob } from "./types/worker";

/**
 * Producer middleware — an onion around every send. Middleware can inspect
 * or mutate the envelope and options (e.g. inject tenant ids, transform
 * payloads) before delegating with `next()`.
 */
export interface SendContext<TTypes extends ProviderTypes = ProviderTypes> {
  readonly queue: QueueRef;
  readonly provider: string;
  readonly capabilities: QueueCapabilities;
  /** Mutate freely before calling `next()`. */
  envelope: QueueEnvelope;
  options: MutableSendOptions<TTypes["send"]>;
}

export type ProducerMiddleware<TTypes extends ProviderTypes = ProviderTypes> = (
  context: SendContext<TTypes>,
  next: () => Promise<SendResult<TTypes["messageId"], TTypes["nativeResult"]>>,
) => Promise<SendResult<TTypes["messageId"], TTypes["nativeResult"]>>;

/**
 * Worker middleware — an onion around every handler execution.
 * `next()` runs the remaining middleware and the handler.
 */
export interface WorkerContext {
  readonly job: QueueJob;
  readonly envelope: QueueEnvelope;
  /** Set by middleware after `next()` if they want to override the result. */
  result?: unknown;
}

export type WorkerMiddleware = (
  context: WorkerContext,
  next: () => Promise<unknown>,
) => Promise<unknown>;

type Next<TResult> = () => Promise<TResult>;

/**
 * Koa-style composition: middleware run in registration order, unwinding in
 * reverse. Built once per queue/worker so the hot path has no per-call
 * allocation beyond closure invocation.
 */
export function compose<TContext, TResult>(
  middleware: readonly ((context: TContext, next: Next<TResult>) => Promise<TResult>)[],
): (context: TContext, last: Next<TResult>) => Promise<TResult> {
  return (context, last) => {
    let index = -1;
    const dispatch = (i: number): Promise<TResult> => {
      if (i <= index) {
        return Promise.reject(new Error("next() called multiple times"));
      }
      index = i;
      const fn = i < middleware.length ? middleware[i] : undefined;
      return fn === undefined ? last() : fn(context, () => dispatch(i + 1));
    };
    return dispatch(0);
  };
}
