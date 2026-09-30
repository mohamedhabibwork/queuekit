import { describe, expect, it } from "vitest";
import { createRedisStreamAcknowledgement, streamEntries } from "../src/drivers/redis.js";

describe("RedisProvider stream acknowledgements", () => {
  it("requeues a retry with its next attempt, then acknowledges the pending entry", async () => {
    const commands: string[][] = [];
    const acknowledgement = createRedisStreamAcknowledgement(
      {
        sendCommand: async <T>(command: string[]) => {
          commands.push(command);
          return "2-0" as T;
        },
      },
      "jobs",
      "workers",
      "1-0",
      { id: "job-1", payload: { invoiceId: "invoice-1" } },
      1,
    );

    await acknowledgement.retry!();

    expect(commands).toEqual([
      [
        "XADD",
        "jobs",
        "*",
        "data",
        JSON.stringify({ id: "job-1", payload: { invoiceId: "invoice-1" }, attempt: 2 }),
      ],
      ["XACK", "jobs", "workers", "1-0"],
    ]);
  });

  it("dead-letters a reject without requeue before acknowledging the entry", async () => {
    const commands: string[][] = [];
    const acknowledgement = createRedisStreamAcknowledgement(
      {
        sendCommand: async <T>(command: string[]) => {
          commands.push(command);
          return "9-0" as T;
        },
      },
      "jobs",
      "workers",
      "1-0",
      { id: "job-1", payload: { invoiceId: "invoice-1" } },
      3,
      "jobs:dead",
    );

    await acknowledgement.reject!();

    expect(commands).toEqual([
      [
        "XADD",
        "jobs:dead",
        "*",
        "data",
        JSON.stringify({
          id: "job-1",
          payload: { invoiceId: "invoice-1" },
          attempt: 3,
          deadLetterOf: { stream: "jobs", id: "1-0", group: "workers" },
        }),
      ],
      ["XACK", "jobs", "workers", "1-0"],
    ]);
  });

  it("requeues instead of dead-lettering when reject asks for a requeue", async () => {
    const commands: string[][] = [];
    const acknowledgement = createRedisStreamAcknowledgement(
      {
        sendCommand: async <T>(command: string[]) => {
          commands.push(command);
          return "2-0" as T;
        },
      },
      "jobs",
      "workers",
      "1-0",
      { id: "job-1", payload: {} },
      1,
      "jobs:dead",
    );

    await acknowledgement.reject!({ requeue: true });

    expect(commands).toEqual([
      ["XADD", "jobs", "*", "data", JSON.stringify({ id: "job-1", payload: {}, attempt: 2 })],
      ["XACK", "jobs", "workers", "1-0"],
    ]);
  });

  it("acknowledges a reject without a configured dead-letter stream", async () => {
    const commands: string[][] = [];
    const acknowledgement = createRedisStreamAcknowledgement(
      {
        sendCommand: async <T>(command: string[]) => {
          commands.push(command);
          return "2-0" as T;
        },
      },
      "jobs",
      "workers",
      "1-0",
      { id: "job-1", payload: {} },
      1,
    );

    await acknowledgement.reject!();

    expect(commands).toEqual([["XACK", "jobs", "workers", "1-0"]]);
  });
});

describe("streamEntries", () => {
  const entry = (id: string, json: string) => [id, ["data", json]];

  it("parses the raw array response shape (node-redis v4)", () => {
    const entries = streamEntries([["stream", [entry("1-0", '{"payload":1}')]]]);
    expect(entries).toEqual([
      { id: "1-0", data: '{"payload":1}', raw: entry("1-0", '{"payload":1}') },
    ]);
  });

  it("parses the object response shape (node-redis v5+ sendCommand)", () => {
    const entries = streamEntries({ stream: [entry("2-0", '{"payload":2}')] });
    expect(entries).toEqual([
      { id: "2-0", data: '{"payload":2}', raw: entry("2-0", '{"payload":2}') },
    ]);
  });

  it("parses every entry of a multi-entry object response", () => {
    const entries = streamEntries({
      stream: [entry("3-0", '{"payload":3}'), entry("4-0", '{"payload":4}')],
    });
    expect(entries.map((e) => e.id)).toEqual(["3-0", "4-0"]);
  });

  it("returns nothing for an empty read (null BLOCK expiry)", () => {
    expect(streamEntries(null)).toEqual([]);
  });
});
