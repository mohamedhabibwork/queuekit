import { parseDuration } from "./duration";
import type { BackoffPolicy } from "./types/options";

/** Normalized (already-parsed) backoff policy used by the worker engine. */
export type ResolvedBackoff =
  | { kind: "fixed"; delayMs: number }
  | { kind: "linear"; delayMs: number; incrementMs: number; maxDelayMs: number | undefined }
  | { kind: "exponential"; delayMs: number; factor: number; maxDelayMs: number | undefined }
  | { kind: "jitter"; delayMs: number; factor: number; maxDelayMs: number | undefined }
  | { kind: "function"; compute: (attempt: number, error: unknown) => number };

export function resolveBackoff(policy: BackoffPolicy | undefined): ResolvedBackoff {
  if (policy === undefined) {
    // Sensible default: exponential from 500ms, doubling, capped at 30s.
    return { kind: "exponential", delayMs: 500, factor: 2, maxDelayMs: 30_000 };
  }
  if (typeof policy === "function") {
    return {
      kind: "function",
      compute: (attempt, error) => parseDuration(policy({ attempt, error })),
    };
  }
  switch (policy.strategy) {
    case "fixed":
      return { kind: "fixed", delayMs: parseDuration(policy.delay) };
    case "linear":
      return {
        kind: "linear",
        delayMs: parseDuration(policy.delay),
        incrementMs: policy.increment === undefined ? parseDuration(policy.delay) : parseDuration(policy.increment),
        maxDelayMs: policy.maxDelay === undefined ? undefined : parseDuration(policy.maxDelay),
      };
    case "exponential":
      return {
        kind: "exponential",
        delayMs: parseDuration(policy.delay),
        factor: policy.factor ?? 2,
        maxDelayMs: policy.maxDelay === undefined ? undefined : parseDuration(policy.maxDelay),
      };
    case "jitter":
      return {
        kind: "jitter",
        delayMs: parseDuration(policy.delay),
        factor: policy.factor ?? 2,
        maxDelayMs: policy.maxDelay === undefined ? undefined : parseDuration(policy.maxDelay),
      };
  }
}

/** Compute the delay before attempt `attempt + 1`. */
export function computeBackoff(backoff: ResolvedBackoff, attempt: number, error: unknown): number {
  switch (backoff.kind) {
    case "fixed":
      return backoff.delayMs;
    case "function":
      return backoff.compute(attempt, error);
    case "linear": {
      const raw = backoff.delayMs + backoff.incrementMs * (attempt - 1);
      return cap(raw, backoff.maxDelayMs);
    }
    case "exponential": {
      const raw = backoff.delayMs * Math.pow(backoff.factor, attempt - 1);
      return cap(raw, backoff.maxDelayMs);
    }
    case "jitter": {
      const raw = cap(backoff.delayMs * Math.pow(backoff.factor, attempt - 1), backoff.maxDelayMs);
      return Math.round(Math.random() * raw);
    }
  }
}

function cap(ms: number, maxMs: number | undefined): number {
  return Math.max(0, Math.min(ms, maxMs ?? Number.POSITIVE_INFINITY));
}
