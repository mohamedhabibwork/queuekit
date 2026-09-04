import type { QueueSchema } from "./schema";

/**
 * Reusable, strongly typed job definition. Job definitions are the preferred
 * way to connect producers and workers: the input type flows into
 * `queue.send(job, data)` and the handler's `job.data` at both ends.
 *
 * ```ts
 * const sendEmail = defineJob<{ to: string }, { id: string }>("email.send");
 * await queue.send(sendEmail, { to: "user@example.com" });      // typed
 * queue.worker(sendEmail, async (job) => job.data.to);          // inferred
 * ```
 */
export interface JobDefinition<TInput = unknown, TResult = unknown> {
  readonly name: string;
  readonly version: number | undefined;
  /** Optional Standard Schema validator for the input (Zod, Valibot, ArkType, TypeBox, ...). */
  readonly input: QueueSchema<TInput> | undefined;
  /** Optional Standard Schema validator for the handler result. */
  readonly result: QueueSchema<TResult> | undefined;
}

export interface DefineJobOptions<TInput, TResult> {
  name: string;
  version?: number | undefined;
  input?: QueueSchema<TInput> | undefined;
  result?: QueueSchema<TResult> | undefined;
}

/** Create a typed job definition from a plain type parameter. */
export function defineJob<TInput = unknown, TResult = unknown>(name: string): JobDefinition<TInput, TResult>;
/** Create a typed job definition with optional version and Standard Schema validators. */
export function defineJob<TInput = unknown, TResult = unknown>(
  options: DefineJobOptions<NoInfer<TInput>, NoInfer<TResult>>,
): JobDefinition<TInput, TResult>;
export function defineJob<TInput = unknown, TResult = unknown>(
  nameOrOptions: string | DefineJobOptions<TInput, TResult>,
): JobDefinition<TInput, TResult> {
  if (typeof nameOrOptions === "string") {
    return { name: nameOrOptions, version: undefined, input: undefined, result: undefined };
  }
  return {
    name: nameOrOptions.name,
    version: nameOrOptions.version,
    input: nameOrOptions.input,
    result: nameOrOptions.result,
  };
}

/** Anything acceptable where a job definition is expected. */
export type JobRef<TData = unknown> = string | JobDefinition<TData, unknown>;

/** Lightweight job reference that does not need to be a full definition. */
export type JobRefLike = string | { readonly name: string; readonly version?: number | undefined };

/** Returns the job definition when the ref carries one (light refs return undefined). */
export function asJobDefinition(ref: JobRefLike): JobDefinition | undefined {
  return typeof ref === "object" && "input" in ref ? (ref as JobDefinition) : undefined;
}

export function jobName(ref: JobRefLike): string {
  return typeof ref === "string" ? ref : ref.name;
}

export function jobVersion(ref: JobRefLike): number | undefined {
  return typeof ref === "string" ? undefined : ref.version;
}
