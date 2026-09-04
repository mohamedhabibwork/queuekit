import { QueueConfigurationError } from "./errors";

/**
 * Portable duration input.
 *
 * A plain number is interpreted as milliseconds. String forms carry their
 * unit explicitly: `"500ms"`, `"30s"`, `"5m"`, `"2h"`, `"7d"`.
 */
export type DurationInput =
  | number
  | `${number}ms`
  | `${number}s`
  | `${number}m`
  | `${number}h`
  | `${number}d`;

const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;

/**
 * Parse a {@link DurationInput} into milliseconds.
 *
 * @throws {QueueConfigurationError} when the string form is malformed.
 */
export function parseDuration(input: DurationInput): number {
  if (typeof input === "number") {
    if (!Number.isFinite(input)) {
      throw new QueueConfigurationError(`Duration must be a finite number, received: ${String(input)}`);
    }
    return input;
  }

  const match = DURATION_PATTERN.exec(input);
  if (match === null) {
    throw new QueueConfigurationError(
      `Invalid duration "${input}". Expected a number (milliseconds) or a string like "500ms", "30s", "5m", "2h", "7d".`,
    );
  }

  const value = Number(match[1]);
  const unit = match[2] as keyof typeof UNIT_MS;
  return value * UNIT_MS[unit]!;
}
