import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MessageEntity } from '../common/entities/message.entity';
import { ChatConsumer } from './chat.consumer';
import { RedisProviders } from '../common/redis/redis.provider';

@Module({
  imports: [TypeOrmModule.forFeature([MessageEntity])],
  providers: [ChatConsumer, ...RedisProviders],
})
export class ConsumerModule {}
