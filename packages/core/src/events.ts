/** Unsubscribe function returned by event registrations. */
export type Unsubscribe = () => void;

export type EventHandler<TPayload> = (payload: TPayload) => void | Promise<void>;

/**
 * Minimal strongly typed event emitter. Zero dependencies, no Node
 * `EventEmitter`, no global state. Handler errors are caught and reported
 * through `onHandlerError` (default: console.error) so a broken observer
 * can never break the pipeline.
 */
export class Emitter<Events extends object> {
  private readonly handlers = new Map<keyof Events, Set<EventHandler<never>>>();
  private readonly onceHandlers = new Map<EventHandler<never>, keyof Events>();

  constructor(
    private readonly onHandlerError: (error: unknown, event: keyof Events) => void = defaultHandlerError as never,
  ) {}

  on<K extends keyof Events & string>(event: K, handler: EventHandler<Events[K]>): Unsubscribe {
    let set = this.handlers.get(event);
    if (set === undefined) {
      set = new Set();
      this.handlers.set(event, set);
    }
    set.add(handler as EventHandler<never>);
    return () => {
      set?.delete(handler as EventHandler<never>);
    };
  }

  once<K extends keyof Events & string>(event: K, handler: EventHandler<Events[K]>): Unsubscribe {
    const wrapped: EventHandler<Events[K]> = (payload) => {
      unsubscribe();
      return handler(payload);
    };
    const unsubscribe = this.on(event, wrapped);
    return unsubscribe;
  }

  off<K extends keyof Events & string>(event: K, handler: EventHandler<Events[K]>): void {
    this.handlers.get(event)?.delete(handler as EventHandler<never>);
    const key = this.onceHandlers.get(handler as EventHandler<never>);
    if (key === event) this.onceHandlers.delete(handler as EventHandler<never>);
  }

  emit<K extends keyof Events & string>(event: K, payload: Events[K]): void {
    const set = this.handlers.get(event);
    if (set === undefined || set.size === 0) return;
    for (const handler of [...set]) {
      try {
        const result = (handler as EventHandler<Events[K]>)(payload);
        if (result instanceof Promise) {
          result.catch((error: unknown) => this.onHandlerError(error, event));
        }
      } catch (error) {
        this.onHandlerError(error, event);
      }
    }
  }

  removeAllListeners(event?: keyof Events & string): void {
    if (event === undefined) {
      this.handlers.clear();
    } else {
      this.handlers.delete(event);
    }
  }

  listenerCount(event: keyof Events & string): number {
    return this.handlers.get(event)?.size ?? 0;
  }
}

function defaultHandlerError(error: unknown, event: unknown): void {
  console.error(`[queue-kit] event handler for "${String(event)}" threw:`, error);
}
