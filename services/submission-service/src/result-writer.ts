import {
  createKafkaClient,
  createConsumer,
  getClient,
  logger,
  closeDb,
  TOPICS,
  JudgingStartedEventSchema,
  TestResultEventSchema,
  FinalVerdictEventSchema,
} from "@bytearena/shared";
import type { Consumer } from "kafkajs";

export interface ResultWriterOptions {
  groupId?: string;
  clientId?: string;
  sessionTimeout?: number;
  closeDbOnStop?: boolean;
}

export class ResultWriter {
  private consumer: Consumer | null = null;
  private isRunning = false;
  private readonly groupId: string;
  private readonly clientId: string;
  private readonly sessionTimeout: number;
  private readonly closeDbOnStop: boolean;

  constructor(options: ResultWriterOptions = {}) {
    this.groupId = options.groupId ?? "result-writer-group";
    this.clientId = options.clientId ?? "result-writer";
    this.sessionTimeout = options.sessionTimeout ?? 30000;
    this.closeDbOnStop = options.closeDbOnStop ?? false;
  }

  async start(): Promise<void> {
    this.isRunning = true;
    logger.info({ clientId: this.clientId, groupId: this.groupId }, "Starting result writer...");

    const kafka = createKafkaClient(this.clientId);
    this.consumer = await createConsumer(kafka, {
      groupId: this.groupId,
      sessionTimeout: this.sessionTimeout,
    });

    await this.consumer.subscribe({
      topic: TOPICS.SUBMISSIONS_RESULTS,
      fromBeginning: true,
    });

    await this.consumer.run({
      // Manual offset commit per message to ensure at-least-once persistence.
      // The primary-key / conditional-update idempotency rules absorb duplicates.
      autoCommit: false,
      eachMessage: async ({ message, partition, topic }) => {
        if (!this.isRunning) return;

        const raw = message.value?.toString("utf8");
        if (!raw) return;

        let parsed: unknown;
        try {
          parsed = JSON.parse(raw);
        } catch {
          logger.warn({ topic, partition, offset: message.offset }, "Skipping non-JSON message in results topic");
          // Still commit so we don't get stuck
          await this.consumer!.commitOffsets([
            { topic, partition, offset: String(Number(message.offset) + 1) },
          ]);
          return;
        }

        const typedRaw = parsed as { type?: unknown };
        const eventType = typedRaw?.type;

        try {
          if (eventType === "JUDGING_STARTED") {
            await this.handleJudgingStarted(parsed);
          } else if (eventType === "TEST_RESULT") {
            await this.handleTestResult(parsed);
          } else if (eventType === "FINAL_VERDICT") {
            await this.handleFinalVerdict(parsed);
          } else {
            logger.warn({ eventType }, "Unknown event type in results topic — skipping");
          }
        } catch (err) {
          // Log but continue — don't block Kafka offset progress on transient DB errors.
          // On restart the message will be redelivered and the idempotent writes will be safe.
          logger.error({ err, eventType }, "Error persisting result event — will retry on redelivery");
          return; // do NOT commit offset so this message is retried after session timeout
        }

        // Commit after successful DB write
        await this.consumer!.commitOffsets([
          { topic, partition, offset: String(Number(message.offset) + 1) },
        ]);
      },
    });

    logger.info({ topic: TOPICS.SUBMISSIONS_RESULTS, groupId: this.groupId }, "Subscribed to results topic, beginning consumption");
  }

  /**
   * JUDGING_STARTED: transition submission from QUEUED → JUDGING.
   * Idempotency: WHERE status='QUEUED' so a repeated event is a no-op.
   */
  private async handleJudgingStarted(raw: unknown): Promise<void> {
    const event = JudgingStartedEventSchema.parse(raw);
    const client = await getClient();
    try {
      const result = await client.query<{ id: string }>(
        `UPDATE submissions
         SET status = 'JUDGING'
         WHERE id = $1 AND status = 'QUEUED'
         RETURNING id`,
        [event.submissionId]
      );
      if (result.rowCount === 0) {
        // Already past QUEUED (JUDGING / COMPLETED / SYSTEM_ERROR) — safe no-op
        logger.debug({ submissionId: event.submissionId }, "JUDGING_STARTED: submission not in QUEUED state, skipping");
      } else {
        logger.info({ submissionId: event.submissionId }, "Updated submission status to JUDGING");
      }
    } finally {
      client.release();
    }
  }

  /**
   * TEST_RESULT: insert per-test verdict row.
   * Idempotency: ON CONFLICT (submission_id, test_index) DO NOTHING — PK prevents duplicates.
   */
  private async handleTestResult(raw: unknown): Promise<void> {
    const event = TestResultEventSchema.parse(raw);
    const client = await getClient();
    try {
      const result = await client.query<{ submission_id: string }>(
        `INSERT INTO test_results (submission_id, test_index, verdict, time_ms, memory_kb, is_sample)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (submission_id, test_index) DO NOTHING
         RETURNING submission_id`,
        [
          event.submissionId,
          event.testIndex,
          event.verdict,
          event.timeMs,
          event.memoryKb,
          event.isSample,
        ]
      );
      if (result.rowCount === 0) {
        logger.debug(
          { submissionId: event.submissionId, testIndex: event.testIndex },
          "TEST_RESULT: duplicate row ignored (idempotent)"
        );
      } else {
        logger.info(
          { submissionId: event.submissionId, testIndex: event.testIndex, verdict: event.verdict },
          "Persisted TEST_RESULT"
        );
      }
    } finally {
      client.release();
    }
  }

  /**
   * FINAL_VERDICT: transition submission to COMPLETED (or SYSTEM_ERROR for INTERNAL_ERROR).
   * Idempotency: WHERE status IN ('QUEUED','JUDGING') so later duplicates are no-ops.
   * First final verdict wins.
   */
  private async handleFinalVerdict(raw: unknown): Promise<void> {
    const event = FinalVerdictEventSchema.parse(raw);
    const newStatus = event.verdict === "INTERNAL_ERROR" ? "SYSTEM_ERROR" : "COMPLETED";
    const client = await getClient();
    try {
      const result = await client.query<{ id: string }>(
        `UPDATE submissions
         SET status = $2, verdict = $3, judged_at = now()
         WHERE id = $1 AND status IN ('QUEUED', 'JUDGING')
         RETURNING id`,
        [event.submissionId, newStatus, event.verdict]
      );
      if (result.rowCount === 0) {
        logger.debug(
          { submissionId: event.submissionId, verdict: event.verdict },
          "FINAL_VERDICT: submission already finalized, skipping (idempotent)"
        );
      } else {
        logger.info(
          { submissionId: event.submissionId, verdict: event.verdict, status: newStatus },
          "Persisted FINAL_VERDICT"
        );
      }
    } finally {
      client.release();
    }
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
    }
    if (this.closeDbOnStop) {
      await closeDb();
    }
    logger.info("Result writer stopped");
  }
}
