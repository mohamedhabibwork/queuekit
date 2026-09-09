import type { QueueMiddleware, QueueOperationContext } from './types.js';

export async function runMiddleware(middleware: readonly QueueMiddleware[], context: QueueOperationContext, operation: () => Promise<void>): Promise<void> {
  let cursor = -1;
  const dispatch = async (index: number): Promise<void> => {
    if (index <= cursor) throw new Error('Queue middleware called next() more than once.');
    cursor = index;
    const current = middleware[index];
    if (current) await current(context, () => dispatch(index + 1)); else await operation();
  };
  await dispatch(0);
}
