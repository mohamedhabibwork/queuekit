import { QueueDeserializationError, QueueSerializationError } from './errors.js';
import type { QueueCodec } from './types.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const jsonCodec: QueueCodec = {
  encode(value) {
    try { return JSON.stringify(value); } catch (cause) { throw new QueueSerializationError('Could not JSON-serialize queue payload.', {}, { cause }); }
  },
  decode(value) {
    try { return JSON.parse(typeof value === 'string' ? value : decoder.decode(value)) as unknown; } catch (cause) { throw new QueueDeserializationError('Could not JSON-deserialize queue payload.', {}, { cause }); }
  },
};
export const textCodec: QueueCodec<string> = { encode: (value) => value, decode: (value) => typeof value === 'string' ? value : decoder.decode(value) };
export const bytesCodec: QueueCodec<Uint8Array> = { encode: (value) => value, decode: (value) => typeof value === 'string' ? encoder.encode(value) : value };

export function asBytes(value: Uint8Array | string): Uint8Array { return typeof value === 'string' ? encoder.encode(value) : value; }
export function asText(value: Uint8Array | string): string { return typeof value === 'string' ? value : decoder.decode(value); }
