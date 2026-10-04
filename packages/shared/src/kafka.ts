import {
  Kafka,
  KafkaConfig,
  Producer,
  ProducerConfig,
  Consumer,
  ConsumerConfig,
  logLevel,
} from "kafkajs";
import { config } from "./config";
import { logger } from "./logger";

export function createKafkaClient(clientId: string, overrides: Partial<KafkaConfig> = {}): Kafka {
  return new Kafka({
    clientId,
    brokers: config.kafkaBrokersArray,
    logLevel: logLevel.NOTHING, // Handled via application logging
    ...overrides,
  });
}

export async function createProducer(
  kafka: Kafka,
  options?: ProducerConfig
): Promise<Producer> {
  const producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    ...options,
  });

  await producer.connect();
  logger.info("Kafka producer connected");
  return producer;
}

export async function createConsumer(
  kafka: Kafka,
  options: ConsumerConfig
): Promise<Consumer> {
  const consumer = kafka.consumer({
    ...options,
  });

  await consumer.connect();
  logger.info({ groupId: options.groupId }, "Kafka consumer connected");
  return consumer;
}

export interface PublishMessageParams {
  producer: Producer;
  topic: string;
  key: string;
  value: unknown;
  headers?: Record<string, string>;
}

export async function publishJsonMessage({
  producer,
  topic,
  key,
  value,
  headers,
}: PublishMessageParams): Promise<void> {
  const messageValue = typeof value === "string" ? value : JSON.stringify(value);
  await producer.send({
    topic,
    messages: [
      {
        key,
        value: messageValue,
        headers,
      },
    ],
  });
}
