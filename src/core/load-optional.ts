import { QueueConfigError } from './errors.js';

/** Loads a provider SDK only when its provider is constructed. */
export async function loadOptional<T>(packageName: string, provider: string): Promise<T> {
  try { return await import(packageName) as T; }
  catch (cause) {
    throw new QueueConfigError(`The "${provider}" provider requires the "${packageName}" package. Install it with: npm install ${packageName}`, { provider }, { cause });
  }
}
