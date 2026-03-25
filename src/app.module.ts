import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UserEntity } from './common/entities/user.entity';
import { ChatRoomEntity } from './common/entities/room.entity';
import { MessageEntity } from './common/entities/message.entity';
import { ConsumerModule } from './consumer/consumer.module';

@Module({
  imports: [
    // Load .env
    ConfigModule.forRoot({ isGlobal: true }),

    // PostgreSQL — sama seperti chat-service
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        type: 'postgres',
        host: config.get('DB_HOST'),
        port: +config.get('DB_PORT'),
        username: config.get('DB_USERNAME'),
        password: config.get('DB_PASSWORD'),
        database: config.get('DB_NAME'),
        entities: [UserEntity, ChatRoomEntity, MessageEntity],
        synchronize: false,
      }),
    }),

    ConsumerModule, // ← module yang akan kita buat selanjutnya
  ],
})
export class AppModule {}