import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(3002); // port berbeda dari chat-service
  console.log('Worker running on port 3002');
}

bootstrap();