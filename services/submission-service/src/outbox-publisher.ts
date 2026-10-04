import {
  createKafkaClient,
  createProducer,
  getClient,
  logger,
  closeDb,
} from "@bytearena/shared";
import type { Producer } from "kafkajs";

export interface OutboxRow {
  id: string; // bigint in pg returned as string
  topic: string;
  msg_key: string;
  payload: unknown;
}

export interface OutboxPublisherOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  clientId?: string;
  closeDbOnStop?: boolean;
}

export class OutboxPublisher {
  private isRunning = false;
  private isProcessing = false;
  private producer: Producer | null = null;
  private pollIntervalMs: number;
  private batchSize: number;
  private clientId: string;
  private closeDbOnStop: boolean;
  private timer: NodeJS.Timeout | null = null;
  private currentBackoffMs = 500;
  private maxBackoffMs = 5000;

  constructor(options: OutboxPublisherOptions = {}) {
    this.pollIntervalMs = options.pollIntervalMs ?? 200;
    this.batchSize = options.batchSize ?? 50;
    this.clientId = options.clientId ?? "outbox-publisher";
    this.closeDbOnStop = options.closeDbOnStop ?? false;
  }

  public async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info({ clientId: this.clientId }, "Starting outbox publisher...");

    const kafka = createKafkaClient(this.clientId);
    this.producer = await createProducer(kafka, {
      idempotent: true,
      maxInFlightRequests: 1,
    });

    this.scheduleNext(0);
  }

  public async stop(): Promise<void> {
    if (!this.isRunning) return;
    this.isRunning = false;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // Wait if a batch is currently in flight
    while (this.isProcessing) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    if (this.producer) {
      try {
        await this.producer.disconnect();
        logger.info("Outbox publisher Kafka producer disconnected");
      } catch (err) {
        logger.warn({ err }, "Error disconnecting Kafka producer during shutdown");
      }
      this.producer = null;
    }

    if (this.closeDbOnStop) {
      await closeDb();
    }
    logger.info("Outbox publisher stopped");
  }

  public async processBatch(): Promise<number> {
    if (!this.producer) {
      throw new Error("Producer not connected");
    }

    const client = await getClient();
    try {
      await client.query("BEGIN");

      // FOR UPDATE SKIP LOCKED ensures multiple concurrent publishers do not process the same rows
      const selectSql = `
        SELECT id, topic, msg_key, payload
        FROM outbox
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED;
      `;
      const res = await client.query<OutboxRow>(selectSql, [this.batchSize]);

      if (res.rows.length === 0) {
        await client.query("COMMIT");
        this.currentBackoffMs = 500; // Reset backoff on successful empty check
        return 0;
      }

      const rows = res.rows;
      logger.debug({ count: rows.length }, "Fetched unpublished outbox rows");

      // Group messages by topic for batch sending
      const topicMap = new Map<string, Array<{ key: string; value: string }>>();
      for (const row of rows) {
        const list = topicMap.get(row.topic) || [];
        const valueStr =
          typeof row.payload === "string" ? row.payload : JSON.stringify(row.payload);
        list.push({ key: row.msg_key, value: valueStr });
        topicMap.set(row.topic, list);
      }

      // Publish to Kafka. If this fails, transaction will be rolled back!
      await this.producer.sendBatch({
        topicMessages: Array.from(topicMap.entries()).map(([topic, messages]) => ({
          topic,
          messages,
        })),
        acks: -1, // Wait for all in-sync replicas to acknowledge
      });

      // Mark published only AFTER broker acknowledged
      const rowIds = rows.map((r) => r.id);
      const updateSql = `
        UPDATE outbox
        SET published_at = now()
        WHERE id = ANY($1::bigint[]);
      `;
      await client.query(updateSql, [rowIds]);

      await client.query("COMMIT");

      logger.info(
        { count: rows.length, firstId: rowIds[0], lastId: rowIds[rowIds.length - 1] },
        "Published outbox batch to Kafka and marked published_at"
      );

      this.currentBackoffMs = 500;
      return rows.length;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  private scheduleNext(delayMs: number): void {
    if (!this.isRunning) return;

    const safeDelay = Math.max(0, delayMs);
    this.timer = setTimeout(async () => {
      this.timer = null;
      if (!this.isRunning) return;

      this.isProcessing = true;
      try {
        const count = await this.processBatch();
        // If we processed a full batch, check immediately for more without waiting
        const nextDelay = count >= this.batchSize ? 0 : this.pollIntervalMs;
        this.scheduleNext(nextDelay);
      } catch (err) {
        logger.error(
          { err, backoffMs: this.currentBackoffMs },
          "Error publishing outbox batch; backing off"
        );
        const delay = this.currentBackoffMs;
        this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);
        this.scheduleNext(delay);
      } finally {
        this.isProcessing = false;
      }
    }, safeDelay);
  }
}

export function startOutboxPublisher(options?: OutboxPublisherOptions): OutboxPublisher {
  const publisher = new OutboxPublisher({ closeDbOnStop: true, ...options });
  publisher.start().catch((err) => {
    logger.fatal({ err }, "Failed to start outbox publisher");
    process.exit(1);
  });
  return publisher;
}

if (require.main === module) {
  const publisher = startOutboxPublisher();

  const handleShutdown = (signal: string) => {
    logger.info({ signal }, "Shutdown signal received for outbox-publisher");
    publisher
      .stop()
      .then(() => process.exit(0))
      .catch((err) => {
        logger.error({ err }, "Error during outbox-publisher shutdown");
        process.exit(1);
      });
  };

  process.on("SIGTERM", () => handleShutdown("SIGTERM"));
  process.on("SIGINT", () => handleShutdown("SIGINT"));
}
