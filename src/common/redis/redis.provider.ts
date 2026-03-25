import { Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

export const REDIS_CLIENT = 'REDIS_CLIENT';

export const RedisProviders: Provider[] = [
  {
    provide: REDIS_CLIENT,
    inject: [ConfigService],
    useFactory: (config: ConfigService) => {
      const redis = new Redis({
        host: config.get('REDIS_HOST', 'localhost'),
        port: +config.get('REDIS_PORT', 6379),
      });
      redis.on('connect', () => console.log('Chat service Worker Redis connected'));
      redis.on('error', (err) => console.error('Chat service Worker Redis error', err));
      return redis;
    },
  },
];