import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    kafka: 'src/kafka.ts',
    rabbitmq: 'src/rabbitmq.ts',
    bullmq: 'src/bullmq.ts',
    redis: 'src/redis.ts',
    nats: 'src/nats.ts',
    sqs: 'src/sqs.ts',
    custom: 'src/custom.ts',
    testing: 'src/testing.ts',
  },
  clean: true,
  dts: true,
  format: ['esm'],
  target: 'es2022',
  sourcemap: true,
  splitting: true,
  treeshake: true,
});
