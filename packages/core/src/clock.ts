import { QueueConfigurationError } from "./errors";

/** Injectable source of time, so tests can fast-forward delays deterministically. */
export interface Clock {
  /** Current time in epoch milliseconds. */
  now(): number;
  /**
   * Resolves after at least `ms` of *this clock's* time has passed.
   * With the {@link FakeClock} the promise only resolves once the fake time
   * has been advanced far enough.
   */
  delay(ms: number, signal?: AbortSignal): Promise<void>;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  delay(ms, signal) {
    return new Promise<void>((resolve, reject) => {
      if (ms <= 0 && signal?.aborted !== true) {
        resolve();
        return;
      }
      const timer = setTimeout(finish, ms);
      guardUnref(timer);

      function finish() {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        if (signal?.aborted) {
          reject(signal.reason instanceof Error ? signal.reason : new Error("Aborted"));
        } else {
          resolve();
        }
      }
      function onAbort() {
        clearTimeout(timer);
        finish();
      }
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  },
};

type FakeTimerState = "scheduled" | "fired" | "cancelled";

export interface FakeTimeout {
  cancel(): void;
  readonly state: FakeTimerState;
}

type AdvanceListener = (fromMs: number, toMs: number) => void;

/**
 * Deterministic clock for tests. Nothing happens until you advance time:
 *
 * ```ts
 * const clock = new FakeClock();
 * const queue = createQueue({ name: "emails", provider: memory({ clock }) });
 * await queue.send("email.send", payload, { delay: "5m" });
 * clock.advance("5m"); // the delayed message becomes visible
 * ```
 */
export class FakeClock implements Clock {
  private currentMs: number;
  private readonly timers = new Set<{
    atMs: number;
    resolve: () => void;
    state: FakeTimerState;
  }>();
  private readonly listeners = new Set<AdvanceListener>();

  constructor(startMs: number = 0) {
    this.currentMs = startMs;
  }

  now(): number {
    return this.currentMs;
  }

  async delay(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      throw signal.reason instanceof Error ? signal.reason : new Error("Aborted");
    }
    if (ms <= 0) return;
    return new Promise<void>((resolve, reject) => {
      const entry = {
        atMs: this.currentMs + ms,
        resolve: () => {
          entry.state = "fired";
          this.timers.delete(entry);
          signal?.removeEventListener("abort", onAbort);
          resolve();
        },
        state: "scheduled" as FakeTimerState,
      };
      this.timers.add(entry);

      function onAbort() {
        if (entry.state === "scheduled") {
          entry.state = "cancelled";
          signal?.removeEventListener("abort", onAbort);
          reject(new Error("Aborted"));
        }
      }
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  /** Move fake time forward by a duration, firing any due timers. */
  advance(duration: number | DurationInput): number {
    const target = this.currentMs + parseDuration(duration);
    return this.advanceTo(target);
  }

  /** Move fake time to an absolute epoch-ms value, firing timers in order. */
  advanceTo(targetMs: number): number {
    if (targetMs < this.currentMs) {
      throw new QueueConfigurationError("FakeClock cannot move backwards");
    }
    const from = this.currentMs;
    this.currentMs = targetMs;

    const due = [...this.timers].filter((t) => t.atMs <= targetMs).sort((a, b) => a.atMs - b.atMs);
    for (const timer of due) {
      if (timer.state === "scheduled") timer.resolve();
    }

    for (const listener of [...this.listeners]) {
      listener(from, targetMs);
    }
    return targetMs - from;
  }

  /**
   * Register a callback fired whenever fake time advances. The memory
   * adapter uses this to release delayed messages deterministically.
   */
  onAdvance(listener: AdvanceListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** Number of pending (not yet fired) timers — useful in assertions. */
  get pendingTimers(): number {
    return [...this.timers].filter((t) => t.state === "scheduled").length;
  }
}

/**
 * Prevent Node/Bun timers from keeping the process alive. Harmless no-op on
 * runtimes (Deno, browsers) whose timers do not expose `unref`.
 */
export function guardUnref(timer: unknown): void {
  const candidate = timer as { unref?: unknown } | number | undefined;
  if (candidate && typeof candidate === "object" && typeof candidate.unref === "function") {
    candidate.unref();
  }
}

import type { DurationInput } from "./duration";
import { parseDuration } from "./duration";
