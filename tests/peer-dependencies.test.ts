import { describe, expect, it } from "vitest";
import { QueueConfigError } from "../src/core/errors.js";
import { loadOptional } from "../src/core/load-optional.js";
import { createQueue } from "../src/index.js";

/**
 * QueueKit must stay useful with zero provider SDKs installed: the memory
 * provider covers development and tests, and every SDK-backed driver loads
 * its optional peer only when that provider is created — with an install
 * hint if the package is missing.
 */
describe("optional peer dependencies", () => {
  it("publishes and consumes with the memory provider and no SDK installed", async () => {
    const queue = await createQueue({ type: "memory" });
    await queue.publish("emails", { type: "welcome", payload: { email: "person@example.com" } });
    const received = new Promise<string>((resolve) => {
      void queue.consume<{ email: string }>("emails", ({ message }) =>
        resolve(message.payload.email),
      );
    });
    await expect(received).resolves.toBe("person@example.com");
    await queue.close();
  });

  it("hints at the install command when a provider SDK is missing", async () => {
    await expect(loadOptional("queuekit-not-an-installed-package", "kafka")).rejects.toThrow(
      QueueConfigError,
    );
    await expect(loadOptional("queuekit-not-an-installed-package", "kafka")).rejects.toThrow(
      /npm install queuekit-not-an-installed-package/,
    );
  });
});
