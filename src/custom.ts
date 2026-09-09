import type { QueueCapabilities, QueueProvider } from './core/types.js';

export interface QueueProviderDefinition<TName extends string, TConfig, TProvider extends QueueProvider<TName>> {
  readonly name: TName;
  readonly capabilities: QueueCapabilities;
  create(config: TConfig): Promise<TProvider> | TProvider;
}
export function defineQueueProvider<TName extends string, TConfig, TProvider extends QueueProvider<TName>>(definition: QueueProviderDefinition<TName, TConfig, TProvider>): QueueProviderDefinition<TName, TConfig, TProvider> { return definition; }

type DefinitionConfig<T> = T extends QueueProviderDefinition<string, infer TConfig, QueueProvider> ? TConfig : never;
type DefinitionProvider<T> = T extends QueueProviderDefinition<string, unknown, infer TProvider> ? TProvider : never;
export type CustomProviderConfig<TDefinitions extends Record<string, QueueProviderDefinition<string, unknown, QueueProvider>>> = { [TKey in keyof TDefinitions]: { readonly type: TKey } & DefinitionConfig<TDefinitions[TKey]> }[keyof TDefinitions];

export function createQueueKit<const TDefinitions extends Record<string, QueueProviderDefinition<string, unknown, QueueProvider>>>(options: { readonly providers: TDefinitions }) {
  return {
    async create<TConfig extends CustomProviderConfig<TDefinitions>>(config: TConfig): Promise<DefinitionProvider<TDefinitions[TConfig['type']]>> {
      const definition = options.providers[config.type] as QueueProviderDefinition<string, object, QueueProvider> | undefined;
      if (!definition) throw new Error(`Unknown QueueKit provider: ${String(config.type)}`);
      const { type: _type, ...providerConfig } = config;
      return definition.create(providerConfig) as Promise<DefinitionProvider<TDefinitions[TConfig['type']]>>;
    },
  };
}
