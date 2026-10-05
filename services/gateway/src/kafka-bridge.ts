import os from "os";
import { EventEmitter } from "events";
import {
  createKafkaClient,
  createConsumer,
  TOPICS,
  SubmissionResultEventSchema,
  SubmissionResultEvent,
  logger,
} from "@bytearena/shared";
import type { Consumer } from "kafkajs";

export class KafkaBridge {
  private consumer: Consumer | null = null;
  private emitter = new EventEmitter();
  private isRunning = false;
  private readonly groupId: string;
  private readonly clientId: string;

  constructor(options: { groupId?: string; clientId?: string } = {}) {
    const host = process.env.HOSTNAME || os.hostname() || "node";
    const rand = Math.floor(Math.random() * 10000);
    this.groupId = options.groupId ?? `gateway-${host}-${rand}`;
    this.clientId = options.clientId ?? `gateway-bridge-${host}-${rand}`;
    // Set max listeners to 0 (unlimited) so many active GraphQL subscriptions can listen
    this.emitter.setMaxListeners(0);
  }

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info(
      { groupId: this.groupId, clientId: this.clientId },
      "Starting Kafka-to-subscription gateway bridge..."
    );

    const kafka = createKafkaClient(this.clientId);
    this.consumer = await createConsumer(kafka, {
      groupId: this.groupId,
      sessionTimeout: 15000,
    });

    await this.consumer.subscribe({
      topic: TOPICS.SUBMISSIONS_RESULTS,
      fromBeginning: false, // live events only
    });

    await this.consumer.run({
      autoCommit: true,
      eachMessage: async ({ message, topic, partition }) => {
        if (!this.isRunning) return;
        const raw = message.value?.toString("utf8");
        if (!raw) return;

        try {
          const parsed = JSON.parse(raw);
          const event = SubmissionResultEventSchema.parse(parsed);

          // Emit to any in-memory listeners waiting on this submissionId
          this.emitter.emit(event.submissionId, event);
        } catch (err) {
          logger.trace(
            { err, topic, partition, offset: message.offset },
            "Non-result or invalid event in submissions.results topic ignored by gateway bridge"
          );
        }
      },
    });

    logger.info(
      { topic: TOPICS.SUBMISSIONS_RESULTS, groupId: this.groupId },
      "Kafka bridge subscribed to results topic"
    );
  }

  subscribe(
    submissionId: string,
    listener: (event: SubmissionResultEvent) => void
  ): () => void {
    this.emitter.on(submissionId, listener);
    return () => {
      this.emitter.off(submissionId, listener);
    };
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    this.emitter.removeAllListeners();
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
    }
    logger.info("Kafka bridge stopped");
  }
}
