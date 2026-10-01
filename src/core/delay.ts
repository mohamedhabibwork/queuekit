import type { PublishOptions } from "./types.js";

/** Effective publish delay in ms: explicit `delay` wins, else time until `scheduleAt` (never negative). */
export function resolveDelay(
  options: Pick<PublishOptions, "delay" | "scheduleAt"> | undefined,
  now: number = Date.now(),
): number | undefined {
  if (options?.delay !== undefined) return options.delay;
  if (options?.scheduleAt) return Math.max(0, options.scheduleAt.getTime() - now);
  return undefined;
}
