export interface QueueErrorContext {
  readonly provider?: string;
  readonly operation?: string;
  readonly destination?: string;
  readonly code?: string;
  readonly retryable?: boolean;
  readonly statusCode?: number;
  readonly native?: unknown;
}

export class QueueError extends Error {
  readonly context: QueueErrorContext;
  constructor(message: string, context: QueueErrorContext = {}, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
    this.context = context;
  }
}
export class QueueConfigError extends QueueError {}
export class QueueConnectionError extends QueueError {}
export class QueueAuthenticationError extends QueueError {}
export class QueueAuthorizationError extends QueueError {}
export class QueuePublishError extends QueueError {}
export class QueueConsumeError extends QueueError {}
export class QueueTimeoutError extends QueueError {}
export class QueueRateLimitError extends QueueError {}
export class QueueSerializationError extends QueueError {}
export class QueueDeserializationError extends QueueError {}
export class QueueUnsupportedFeatureError extends QueueError {}
export class QueueClosedError extends QueueError {}

export function isRetryableQueueError(error: unknown): error is QueueError {
  return error instanceof QueueError && error.context.retryable === true;
}
