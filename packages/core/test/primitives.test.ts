import { describe, expect, it } from "vitest";
import { parseDuration } from "../src/duration";
import { FakeClock, systemClock } from "../src/clock";
import {
  fatal,
  isFatalError,
  QueueConfigurationError,
  retryable,
  UnsupportedCapabilityError,
  QueueKitError,
  QueueSendError,
  QueueValidationError,
} from "../src/errors";
import { JsonSerializer } from "../src/serializer";
import { ENVELOPE_VERSION, isEnvelopeLike } from "../src/envelope";
import { createCapabilities } from "../src/capabilities";
import { computeBackoff, resolveBackoff } from "../src/backoff";
import { Emitter } from "../src/events";
import { defineJob, jobName, jobVersion } from "../src/job";
import { createId } from "../src/id";

describe("parseDuration", () => {
  it("treats numbers as milliseconds", () => {
    expect(parseDuration(1500)).toBe(1500);
  });

  it("parses all unit suffixes", () => {
    expect(parseDuration("500ms")).toBe(500);
    expect(parseDuration("30s")).toBe(30_000);
    expect(parseDuration("5m")).toBe(300_000);
    expect(parseDuration("2h")).toBe(7_200_000);
    expect(parseDuration("7d")).toBe(604_800_000);
    expect(parseDuration("1.5s")).toBe(1500);
  });

  it("rejects malformed durations with QueueConfigurationError", () => {
    expect(() => parseDuration("30seconds" as never)).toThrow(QueueConfigurationError);
    expect(() => parseDuration(Number.NaN)).toThrow(QueueConfigurationError);
  });
});

describe("FakeClock", () => {
  it("only advances time explicitly", async () => {
    const clock = new FakeClock(1_000);
    expect(clock.now()).toBe(1_000);

    let fired = false;
    const done = clock.delay(500).then(() => {
      fired = true;
    });
    await Promise.resolve();
    expect(fired).toBe(false);

    clock.advance(500);
    await done;
    expect(fired).toBe(true);
    expect(clock.now()).toBe(1_500);
  });

  it("supports string durations in advance()", () => {
    const clock = new FakeClock();
    clock.advance("5m");
    expect(clock.now()).toBe(300_000);
  });

  it("notifies onAdvance listeners", () => {
    const clock = new FakeClock();
    const seen: number[] = [];
    clock.onAdvance((_from, to) => seen.push(to));
    clock.advance(100);
    expect(seen).toEqual([100]);
  });

  it("rejects moving backwards", () => {
    const clock = new FakeClock(100);
    expect(() => clock.advanceTo(50)).toThrow(QueueConfigurationError);
  });

  it("system clock is real", async () => {
    const start = systemClock.now();
    await systemClock.delay(5);
    expect(systemClock.now()).toBeGreaterThanOrEqual(start);
  });
});

describe("errors", () => {
  it("preserve the provider cause", () => {
    const cause = new Error("boom");
    const error = new QueueSendError("send failed", { cause, provider: "memory" });
    expect(error.cause).toBe(cause);
    expect(error.provider).toBe("memory");
    expect(error.code).toBe("QUEUE_SEND");
    expect(error.retryable).toBe(true);
    expect(error instanceof QueueKitError).toBe(true);
  });

  it("classify fatal vs retryable", () => {
    expect(isFatalError(retryable(new Error("x")))).toBe(false);
    expect(isFatalError(fatal(new Error("x")))).toBe(true);
    expect(isFatalError(new QueueValidationError("bad"))).toBe(true);
    expect(isFatalError(new UnsupportedCapabilityError("fifo"))).toBe(true);
    expect(isFatalError(new Error("plain"))).toBe(false);
  });

  it("wrapping helpers keep the original error", () => {
    const original = new Error("disk on fire");
    const wrapped = retryable(original, "handled");
    expect(wrapped.message).toBe("handled");
    expect(wrapped.cause).toBe(original);
  });
});

describe("JsonSerializer", () => {
  const serializer = new JsonSerializer();

  it("roundtrips payloads and envelopes", () => {
    const envelope = {
      v: ENVELOPE_VERSION,
      id: "abc",
      name: "email.send",
      payload: { to: "a@b.c" },
      timestamp: 1,
      attempt: 1,
    };
    const encoded = serializer.encode(envelope);
    expect(typeof encoded).toBe("string");
    const decoded = serializer.decode<typeof envelope>(encoded);
    expect(decoded).toEqual(envelope);
  });

  it("exposes envelope detection", () => {
    expect(isEnvelopeLike({ v: 1, id: "x", name: "n", payload: 1 })).toBe(true);
    expect(isEnvelopeLike({ nope: true })).toBe(false);
    expect(isEnvelopeLike(null)).toBe(false);
  });
});

describe("capabilities", () => {
  it("default to unsupported", () => {
    const caps = createCapabilities();
    expect(caps.send.supported).toBe(false);
    expect(caps.delayedDelivery.details).toBeUndefined();
  });

  it("apply typed overrides with details", () => {
    const caps = createCapabilities({
      send: { supported: true, details: { maxBytes: 100, supportsBinary: false } },
      delayedDelivery: { supported: true, mode: "native", details: { maxDelayMs: 900_000 } },
      retries: { supported: true, details: { strategy: "queue-kit" } },
    });
    expect(caps.send.details?.maxBytes).toBe(100);
    expect(caps.delayedDelivery.mode).toBe("native");
    expect(caps.retries.details?.strategy).toBe("queue-kit");
    expect(caps.fifo.supported).toBe(false);
  });
});

describe("backoff", () => {
  it("supports fixed, linear and exponential strategies", () => {
    expect(computeBackoff(resolveBackoff({ strategy: "fixed", delay: "1s" }), 1, null)).toBe(1_000);
    expect(computeBackoff(resolveBackoff({ strategy: "linear", delay: 100 }), 3, null)).toBe(300);
    expect(computeBackoff(resolveBackoff({ strategy: "exponential", delay: 100 }), 3, null)).toBe(400);
  });

  it("caps exponential growth", () => {
    expect(computeBackoff(resolveBackoff({ strategy: "exponential", delay: 100, maxDelay: "1s" }), 10, null)).toBe(1_000);
  });

  it("supports custom functions", () => {
    const policy = resolveBackoff(({ attempt }) => `${attempt}s` as `${number}s`);
    expect(computeBackoff(policy, 4, null)).toBe(4_000);
  });

  it("defaults to exponential 500ms doubling capped at 30s", () => {
    expect(computeBackoff(resolveBackoff(undefined), 1, null)).toBe(500);
    expect(computeBackoff(resolveBackoff(undefined), 2, null)).toBe(1_000);
    expect(computeBackoff(resolveBackoff(undefined), 20, null)).toBe(30_000);
  });
});

describe("Emitter", () => {
  it("delivers typed events and supports unsubscribe", () => {
    type Events = { ping: { n: number }; pong: undefined };
    const emitter = new Emitter<Events>();
    const seen: number[] = [];

    const off = emitter.on("ping", ({ n }) => {
      seen.push(n);
    });
    emitter.emit("ping", { n: 1 });
    off();
    emitter.emit("ping", { n: 2 });
    expect(seen).toEqual([1]);
  });

  it("swallows observer errors without breaking the pipeline", () => {
    const errors: unknown[] = [];
    const emitter = new Emitter<{ boom: undefined }>((error) => errors.push(error));
    emitter.on("boom", () => {
      throw new Error("observer blew up");
    });
    emitter.emit("boom", undefined);
    expect(errors).toHaveLength(1);
  });

  it("supports once()", () => {
    const emitter = new Emitter<{ tick: number }>();
    const seen: number[] = [];
    emitter.once("tick", (n) => {
      seen.push(n);
    });
    emitter.emit("tick", 1);
    emitter.emit("tick", 2);
    expect(seen).toEqual([1]);
  });
});

describe("defineJob", () => {
  it("creates typed job definitions", () => {
    const job = defineJob<{ to: string }, { id: string }>("email.send");
    expect(job.name).toBe("email.send");
    expect(job.version).toBeUndefined();
    expect(job.input).toBeUndefined();
    expect(jobName(job)).toBe("email.send");
    expect(jobVersion(job)).toBeUndefined();
  });

  it("supports versions and validators", () => {
    const schema = {
      "~standard": {
        version: 1 as const,
        vendor: "test",
        validate: (value: unknown) => ({ value: value as { to: string } }),
      },
    };
    const job = defineJob({ name: "email.send", version: 2, input: schema });
    expect(job.version).toBe(2);
    expect(job.input).toBe(schema);
    expect(jobVersion(job)).toBe(2);
  });
});

describe("createId", () => {
  it("produces unique ids", () => {
    const ids = new Set(Array.from({ length: 1_000 }, () => createId()));
    expect(ids.size).toBe(1_000);
  });
});
