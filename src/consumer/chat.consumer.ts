import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer } from 'kafkajs';
import { MessageEntity, MessageType } from '../common/entities/message.entity';
import { REDIS_CLIENT } from '../common/redis/redis.provider';
import Redis from 'ioredis';

@Injectable()
export class ChatConsumer implements OnModuleInit {
  private readonly logger = new Logger(ChatConsumer.name);
  private consumer: Consumer;

  constructor(
    @InjectRepository(MessageEntity)
    private messageRepo: Repository<MessageEntity>,

    @Inject(REDIS_CLIENT)
    private redis: Redis,

    private config: ConfigService,
  ) {
    // Setup Kafka Consumer
    const kafka = new Kafka({
      clientId: 'worker',
      brokers: [this.config.get('KAFKA_BROKER', 'localhost:9092')],
    });

    this.consumer = kafka.consumer({
      groupId: 'chat-worker-group', // group id untuk load balancing
    });
  }

  // ─────────────────────────────────────────────────────
  // onModuleInit → connect dan mulai consume saat worker start
  // Ingat: seperti robot yang langsung standby saat dinyalakan!
  // ─────────────────────────────────────────────────────
  async onModuleInit() {
    await this.consumer.connect();
    this.logger.log('Kafka Consumer connected');

    // Subscribe ke topic chat.inbound
    await this.consumer.subscribe({
      topic: 'chat.inbound',
      fromBeginning: false, // hanya consume pesan baru
    });

    // Mulai consume — ini yang bikin worker selalu standby
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const payload = JSON.parse(message.value.toString());
          this.logger.log(`Pesan diterima dari Kafka: ${JSON.stringify(payload)}`);

          // Step 1: Simpan pesan ke PostgreSQL
          await this.persistMessage(payload);

          // Step 2: Publish ke Redis agar client dapat new_message
          await this.notifyClients(payload);

        } catch (error) {
          this.logger.error('Error processing message', error);
        }
      },
    });
  }

  // ─────────────────────────────────────────────────────
  // Simpan pesan ke DB
  // ─────────────────────────────────────────────────────
  private async persistMessage(payload: {
    room_id: string;
    sender_id: string;
    content: string;
    message_type: MessageType;
    idempotency_key: string;
  }) {
    // Cek duplicate di DB (double check selain Redis)
    const existing = await this.messageRepo.findOne({
      where: { idempotency_key: payload.idempotency_key },
    });

    if (existing) {
      this.logger.warn(`Duplicate di DB: ${payload.idempotency_key}`);
      return existing;
    }

    const message = this.messageRepo.create({
      room_id: payload.room_id,
      sender_id: payload.sender_id,
      content: payload.content,
      message_type: payload.message_type || MessageType.TEXT,
      idempotency_key: payload.idempotency_key,
    });

    const saved = await this.messageRepo.save(message);
    this.logger.log(`Pesan disimpan ke DB: ${saved.id}`);

    return saved;
  }

  // ─────────────────────────────────────────────────────
  // Publish ke Redis channel agar Gateway broadcast ke client
  // ─────────────────────────────────────────────────────
  private async notifyClients(payload: any) {
    await this.redis.publish(
      `room:${payload.room_id}`,
      JSON.stringify({
        event: 'new_message',
        room_id: payload.room_id,
        message: payload,
      }),
    );

    this.logger.log(`Publish ke Redis channel room:${payload.room_id}`);
  }
}