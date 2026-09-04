import { QueueValidationError } from "./errors";

/**
 * Minimal structural subset of the Standard Schema spec
 * (https://standardschema.dev). Zod, Valibot, ArkType and TypeBox schemas
 * already satisfy this shape, so Queue Kit stays validator-agnostic with
 * zero runtime dependencies.
 */
export interface QueueSchema<Output> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown,
    ) =>
      | { readonly value?: Output; readonly issues?: readonly StandardIssue[] }
      | Promise<{ readonly value?: Output; readonly issues?: readonly StandardIssue[] }>;
  };
}

export interface StandardIssue {
  readonly message: string;
  readonly path?: readonly PropertyKey[];
}

export interface ValidationOutcome<T> {
  readonly value: T;
  readonly issues: readonly string[];
}

/**
 * Validate a value against a Standard Schema compatible validator.
 * @throws {QueueValidationError} when validation fails.
 */
export async function validateWithSchema<T>(
  schema: QueueSchema<T>,
  value: unknown,
  context: string,
): Promise<T> {
  const outcome = await schema["~standard"].validate(value);
  if (outcome.issues !== undefined && outcome.issues.length > 0) {
    const messages = outcome.issues.map(formatIssue);
    throw new QueueValidationError(
      `Payload for "${context}" failed validation: ${messages.join("; ")}`,
      { issues: messages },
    );
  }
  return outcome.value as T;
}

function formatIssue(issue: StandardIssue): string {
  const path = issue.path === undefined ? "" : issue.path.map(String).join(".");
  return path.length > 0 ? `${path}: ${issue.message}` : issue.message;
}
