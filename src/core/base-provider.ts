import { QueueClosedError } from './errors.js';
import { runMiddleware } from './middleware.js';
import type { QueueCapabilities, QueueHealth, QueueMiddleware, QueueOperationContext } from './types.js';

export abstract class BaseQueueProvider<TName extends string> {
  abstract readonly name: TName;
  abstract readonly capabilities: QueueCapabilities;
  #closed = false;
  #middleware: QueueMiddleware[] = [];

  use(middleware: QueueMiddleware): this { this.#middleware.push(middleware); return this; }
  protected async operation(context: QueueOperationContext, action: () => Promise<void>): Promise<void> {
    if (this.#closed) throw new QueueClosedError(`The ${this.name} provider is closed.`, { provider: this.name, operation: context.operation, destination: context.destination });
    await runMiddleware(this.#middleware, context, action);
  }
  protected markClosed(): void { this.#closed = true; }
  async health(): Promise<QueueHealth<TName>> { return { ok: !this.#closed, provider: this.name }; }
}
