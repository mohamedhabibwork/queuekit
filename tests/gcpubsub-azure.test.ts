import { describe, expect, it } from "vitest";
import { QueueConfigError } from "../src/core/errors.js";
import { createQueue } from "../src/index.js";

/**
 * The SDK-backed cloud providers get their full round-trips from live-broker
 * e2e runs. Here we pin the factory mapping, the metadata contract, and the
 * config validation that fires before any network call.
 */
describe("gcpubsub provider", () => {
  it("maps the factory config to the provider", async () => {
    const provider = await createQueue({ type: "gcpubsub", projectId: "p", topic: "events" });
    expect(provider.name).toBe("gcpubsub");
    expect(provider.capabilities.kind).toBe("pubsub");
    expect(provider.capabilities.delayed).toBe(false); // Pub/Sub has no per-message delay
    expect(provider.capabilities.deadLetter).toBe(true);
    const health = await provider.health();
    expect(health).toMatchObject({ ok: true, provider: "gcpubsub" });
    expect(provider.native()).toBeUndefined();
    await provider.close();
  });
});

describe("azureservicebus provider", () => {
  it("maps the factory config to the provider", async () => {
    const provider = await createQueue({
      type: "azureservicebus",
      connectionString: "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKey=k",
    });
    expect(provider.name).toBe("azureservicebus");
    expect(provider.capabilities.kind).toBe("queue");
    expect(provider.capabilities.scheduling).toBe(true);
    expect(provider.capabilities.deadLetter).toBe(true);
    await provider.close();
  });

  it("rejects publish when neither connection string nor namespace+credential is set", async () => {
    const provider = await createQueue({ type: "azureservicebus" });
    await expect(provider.publish("jobs", { payload: 1 })).rejects.toThrow(QueueConfigError);
  });
});
