let counter = 0;

/**
 * Runtime-agnostic unique id. Uses WebCrypto `randomUUID` where available
 * (Node 19+, Bun, Deno, browsers) and falls back to a monotonic counter plus
 * randomness elsewhere, so core never depends on a runtime-specific module.
 */
export function createId(): string {
  const cryptoRef = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (typeof cryptoRef?.randomUUID === "function") {
    return cryptoRef.randomUUID();
  }
  counter = (counter + 1) % Number.MAX_SAFE_INTEGER;
  const time = Date.now().toString(36);
  const rand = Math.random().toString(36).slice(2, 10);
  return `${time}-${counter.toString(36)}-${rand}`;
}
