/**
 * Pluggable serialization. The default is JSON; adapters convert the encoded
 * string/bytes into the provider's message body format.
 */
export interface QueueSerializer {
  encode(value: unknown): string | Uint8Array;
  decode<T = unknown>(value: string | Uint8Array): T;
  /** Content type marker stored in envelope metadata when known. */
  readonly contentType?: string;
}

/**
 * JSON serializer — safe, universal, zero dependencies. `undefined` payloads
 * encode as `null` so messages always carry a well-formed body.
 */
export class JsonSerializer implements QueueSerializer {
  readonly contentType = "application/json";

  encode(value: unknown): string {
    return JSON.stringify(value === undefined ? null : value, nullReplacer);
  }

  decode<T = unknown>(value: string | Uint8Array): T {
    if (value instanceof Uint8Array) {
      return JSON.parse(new TextDecoder().decode(value)) as T;
    }
    return JSON.parse(value) as T;
  }
}

function nullReplacer(_key: string, value: unknown): unknown {
  return value;
}
