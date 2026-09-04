/**
 * Base error for everything Queue Kit raises.
 *
 * Rules:
 * - the original provider error is always preserved on `cause`, never discarded;
 * - `retryable` gives callers a coarse classification without sniffing messages;
 * - `provider` names the adapter that produced the error, when known.
 */
export class QueueKitError extends Error {
  readonly code: string;
  readonly provider?: string | undefined;
  readonly retryable: boolean;

  constructor(
    message: string,
    options: QueueKitErrorOptions = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = new.target.name;
    this.code = options.code ?? "QUEUE_KIT_ERROR";
    this.retryable = options.retryable ?? false;
    this.provider = options.provider;
  }
}

export interface QueueKitErrorOptions {
  code?: string | undefined;
  cause?: unknown;
  provider?: string;
  retryable?: boolean;
}

export class QueueConnectionError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_CONNECTION", retryable: true, ...options });
  }
}

export class QueueSendError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_SEND", retryable: true, ...options });
  }
}

export class QueueReceiveError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_RECEIVE", retryable: true, ...options });
  }
}

export class QueueTimeoutError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_TIMEOUT", retryable: true, ...options });
  }
}

export class QueueSerializationError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_SERIALIZATION", retryable: false, ...options });
  }
}

export class QueueValidationError extends QueueKitError {
  /** Human readable list of schema issues, when a validator produced them. */
  readonly issues: readonly string[];

  constructor(
    message: string,
    options: QueueKitErrorOptions & { issues?: readonly string[] } = {},
  ) {
    super(message, { code: "QUEUE_VALIDATION", retryable: false, ...options });
    this.issues = options.issues ?? [];
  }
}

export class QueueAuthenticationError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_AUTHENTICATION", retryable: false, ...options });
  }
}

export class QueueRateLimitError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_RATE_LIMIT", retryable: true, ...options });
  }
}

export class QueueMessageTooLargeError extends QueueKitError {
  readonly sizeBytes: number | undefined;
  readonly maxBytes: number | undefined;

  constructor(
    message: string,
    options: QueueKitErrorOptions & { sizeBytes?: number; maxBytes?: number } = {},
  ) {
    super(message, { code: "QUEUE_MESSAGE_TOO_LARGE", retryable: false, ...options });
    this.sizeBytes = options.sizeBytes;
    this.maxBytes = options.maxBytes;
  }
}

export class UnsupportedCapabilityError extends QueueKitError {
  readonly capability: string;

  constructor(capability: string, message?: string, options: QueueKitErrorOptions = {}) {
    super(
      message ??
        `Capability "${capability}" is not supported by this provider. Use queue.supports("${capability}") to check before relying on it.`,
      { code: "QUEUE_UNSUPPORTED_CAPABILITY", retryable: false, ...options },
    );
    this.capability = capability;
  }
}

export class QueueConfigurationError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_CONFIGURATION", retryable: false, ...options });
  }
}

export class QueueWorkerError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_WORKER", retryable: true, ...options });
  }
}

export class QueueAckError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_ACK", retryable: true, ...options });
  }
}

/** Thrown by application handlers to mark a failure as retry-safe. */
export class QueueRetryableError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_RETRYABLE", retryable: true, ...options });
  }
}

/** Thrown by application handlers to skip every remaining retry attempt. */
export class QueueFatalError extends QueueKitError {
  constructor(message: string, options: QueueKitErrorOptions = {}) {
    super(message, { code: "QUEUE_FATAL", retryable: false, ...options });
  }
}

/** Wrap an unknown thrown value so the worker treats it as retry-safe. */
export function retryable(error: unknown, message?: string): QueueRetryableError {
  if (error instanceof QueueRetryableError) return error;
  return new QueueRetryableError(message ?? describe(error), { cause: error });
}

/** Wrap an unknown thrown value so the worker skips all remaining retries. */
export function fatal(error: unknown, message?: string): QueueFatalError {
  if (error instanceof QueueFatalError) return error;
  return new QueueFatalError(message ?? describe(error), { cause: error });
}

/**
 * Fatal errors (and non-retryable Queue Kit errors) must skip retry.
 * Recognises Queue Kit errors, and duck-types a `retryable: false` property
 * so plain application errors can opt out of retries without wrapping.
 */
export function isFatalError(error: unknown): boolean {
  if (error instanceof QueueFatalError) return true;
  if (error instanceof QueueKitError) return !error.retryable;
  if (typeof error === "object" && error !== null && "retryable" in error) {
    return (error as { retryable?: unknown }).retryable === false;
  }
  return false;
}

function describe(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}
