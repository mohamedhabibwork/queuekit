/**
 * Protocol version stamped on every envelope. Bump when the wire format
 * changes in a way consumers must detect.
 */
export const ENVELOPE_VERSION = 1 as const;

export interface TraceContext {
  traceId?: string;
  spanId?: string;
  parentSpanId?: string;
  baggage?: Record<string, string>;
}

/**
 * Provider-independent message envelope. It travels inside the provider
 * message body so portable features (attempt tracking, metadata, tracing)
 * work identically on every backend.
 */
export interface QueueEnvelope<T = unknown> {
  /** Wire format version. */
  v: number;
  /** Queue Kit message id — stable across retries. */
  id: string;
  /** Logical job name, e.g. "email.send". */
  name: string;
  /** Optional job definition version. */
  version?: number | undefined;
  payload: T;
  /** Epoch milliseconds at publish time (producer's clock). */
  timestamp: number;
  /** Delivery attempt counter; 1 on first delivery. */
  attempt: number;
  metadata?: Record<string, string> | undefined;
  trace?: TraceContext | undefined;
}

/** Anything that looks like an envelope on the wire. */
export type EnvelopeLike = QueueEnvelope;

export function isEnvelopeLike(value: unknown): value is QueueEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    "v" in value &&
    "id" in value &&
    "name" in value &&
    "payload" in value &&
    typeof (value as QueueEnvelope).id === "string" &&
    typeof (value as QueueEnvelope).name === "string"
  );
}

/**
 * Message handed to an adapter: the logical envelope plus its serialized
 * body. Adapters map `body` onto the provider message (e.g. SQS
 * `MessageBody`) — they never re-serialize payloads themselves.
 */
export interface OutboundMessage<T = unknown> {
  readonly envelope: QueueEnvelope<T>;
  /** Serialized envelope body, ready for the provider wire. */
  readonly body: string | Uint8Array;
}
