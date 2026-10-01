import type { RetryPolicy } from "./types.js";

/** Delay in milliseconds before the next attempt after `attempt` (1-based) failed. */
export function computeBackoff(policy: RetryPolicy, attempt: number): number {
  const backoff = policy.backoff;
  if (!backoff) return 0;
  if (backoff.type === "fixed") return backoff.delay;
  const delay = backoff.delay * 2 ** Math.max(0, attempt - 1);
  return backoff.maxDelay === undefined ? delay : Math.min(delay, backoff.maxDelay);
}
