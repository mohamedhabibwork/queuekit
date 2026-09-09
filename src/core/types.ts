export type QueueKind = 'job' | 'queue' | 'pubsub' | 'stream' | 'hybrid';

export interface QueueCapabilities {
  readonly kind: QueueKind;
  readonly publish: boolean;
  readonly consume: boolean;
  readonly batchPublish?: boolean;
  readonly delayed?: boolean;
  readonly retries?: boolean;
  readonly priority?: boolean;
  readonly ttl?: boolean;
  readonly deadLetter?: boolean;
  readonly ack?: boolean;
  readonly nack?: boolean;
  readonly replay?: boolean;
  readonly partitions?: boolean;
  readonly consumerGroups?: boolean;
  readonly transactions?: boolean;
  readonly ordering?: boolean;
  readonly scheduling?: boolean;
  readonly requestReply?: boolean;
}

export interface QueueMessage<TPayload = unknown, THeaders extends Record<string, unknown> = Record<string, unknown>> {
  readonly id?: string;
  readonly type?: string;
  readonly payload: TPayload;
  readonly headers?: THeaders;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly traceId?: string;
  readonly timestamp?: number;
  /** Application-only metadata. Drivers never send this to a broker automatically. */
  readonly metadata?: Record<string, unknown>;
}

export interface RetryPolicy {
  readonly attempts?: number;
  readonly backoff?:
    | { readonly type: 'fixed'; readonly delay: number }
    | { readonly type: 'exponential'; readonly delay: number; readonly maxDelay?: number };
}

export interface PublishOptions<TNative = unknown> {
  readonly delay?: number;
  readonly priority?: number;
  readonly ttl?: number;
  readonly scheduleAt?: Date;
  readonly idempotencyKey?: string;
  readonly retry?: RetryPolicy;
  readonly signal?: AbortSignal;
  readonly native?: TNative;
}

export interface PublishResult<TProvider extends string = string, TNative = unknown> {
  readonly ok: boolean;
  readonly provider: TProvider;
  readonly messageId?: string;
  readonly partition?: number;
  readonly offset?: string;
  readonly sequenceNumber?: string;
  readonly native: TNative;
}

export interface ConsumedMessage<TPayload = unknown, TNativeMessage = unknown> {
  readonly id?: string;
  readonly type?: string;
  readonly payload: TPayload;
  readonly headers: Record<string, unknown>;
  readonly attempt?: number;
  readonly timestamp?: number;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly traceId?: string;
  readonly native: TNativeMessage;
}

export interface RetryOptions { readonly delay?: number; readonly native?: unknown; }
export interface RejectOptions { readonly requeue?: boolean; readonly native?: unknown; }

export interface QueueAcknowledgement<TNative = unknown> {
  readonly native: TNative;
  complete(): Promise<void>;
  retry?(options?: RetryOptions): Promise<void>;
  reject?(options?: RejectOptions): Promise<void>;
}

export interface MessageContext<TPayload = unknown, TNativeMessage = unknown, TAcknowledgement extends QueueAcknowledgement = QueueAcknowledgement> {
  readonly message: ConsumedMessage<TPayload, TNativeMessage>;
  readonly ack: TAcknowledgement;
  readonly signal: AbortSignal;
}

export interface ConsumerOptions<TNative = unknown> {
  readonly concurrency?: number;
  readonly autoAck?: boolean;
  readonly timeout?: number;
  readonly native?: TNative;
}

export type ConsumerStatus = 'starting' | 'running' | 'paused' | 'closing' | 'closed';
export interface QueueConsumer {
  readonly status: ConsumerStatus;
  pause(): Promise<void>;
  resume(): Promise<void>;
  close(): Promise<void>;
}

export type QueueHandler<TPayload, TNativeMessage, TAck extends QueueAcknowledgement> =
  (context: MessageContext<TPayload, TNativeMessage, TAck>) => Promise<void> | void;

export interface QueueHealth<TProvider extends string = string, TNative = unknown> {
  readonly ok: boolean;
  readonly provider: TProvider;
  readonly latencyMs?: number;
  readonly native?: TNative;
}

export interface QueueProvider<TName extends string = string, TPublishNative = unknown, TPublishResponse = unknown, TConsumeNative = unknown, TAck extends QueueAcknowledgement = QueueAcknowledgement, TConsumerNative = unknown> {
  readonly name: TName;
  readonly capabilities: QueueCapabilities;
  publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<TPublishNative>): Promise<PublishResult<TName, TPublishResponse>>;
  publishMany?<TPayload>(destination: string, messages: readonly QueueMessage<TPayload>[], options?: PublishOptions<TPublishNative>): Promise<readonly PublishResult<TName, TPublishResponse>[]>;
  consume<TPayload>(destination: string, handler: QueueHandler<TPayload, TConsumeNative, TAck>, options?: ConsumerOptions<TConsumerNative>): Promise<QueueConsumer>;
  health?(): Promise<QueueHealth<TName>>;
  native(): unknown;
  close(): Promise<void>;
}

export interface QueueCodec<T = unknown> {
  encode(value: T): Uint8Array | string;
  decode(value: Uint8Array | string): T;
}

export interface QueueOperationContext {
  readonly provider: string;
  readonly operation: 'publish' | 'publishMany' | 'consume' | 'ack' | 'retry' | 'reject';
  readonly destination: string;
  readonly traceId?: string;
  readonly correlationId?: string;
  readonly metadata?: Record<string, unknown>;
}

export type QueueMiddleware = (context: QueueOperationContext, next: () => Promise<void>) => Promise<void>;
