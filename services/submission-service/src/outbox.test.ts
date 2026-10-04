import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  query,
  closeDb,
  TOPICS,
  createKafkaClient,
  createConsumer,
  SubmissionQueuedEvent,
} from "@bytearena/shared";
import { OutboxPublisher } from "./outbox-publisher";
import type { Consumer } from "kafkajs";

describe("Outbox Publisher", () => {
  let consumer: Consumer;
  const receivedKeys: string[] = [];

  beforeAll(async () => {
    const kafka = createKafkaClient("outbox-test-verifier");
    consumer = await createConsumer(kafka, {
      groupId: `test-verifier-${Date.now()}`,
    });
    await consumer.subscribe({
      topic: TOPICS.SUBMISSIONS_QUEUED,
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (message.key) {
          receivedKeys.push(message.key.toString());
        }
      },
    });
  });

  afterAll(async () => {
    if (consumer) {
      await consumer.disconnect();
    }
    await closeDb();
  });

  it("publishes pending outbox rows and marks published_at", async () => {
    // 1. Insert an outbox row
    const testSubId = `11111111-2222-3333-4444-${Date.now().toString().slice(-12)}`;
    const eventPayload: SubmissionQueuedEvent = {
      submissionId: testSubId,
      problemId: "sum-two",
      language: "PYTHON",
      createdAt: new Date().toISOString(),
    };

    const insertSql = `
      INSERT INTO outbox (topic, msg_key, payload)
      VALUES ($1, $2, $3)
      RETURNING id;
    `;
    const insertRes = await query<{ id: string }>(insertSql, [
      TOPICS.SUBMISSIONS_QUEUED,
      testSubId,
      JSON.stringify(eventPayload),
    ]);
    const outboxId = insertRes.rows[0]?.id;
    expect(outboxId).toBeDefined();

    // 2. Process batch using publisher
    const publisher = new OutboxPublisher({ clientId: "test-single-publisher" });
    await publisher.start();

    // Give it a moment to run
    const startTime = Date.now();
    while (Date.now() - startTime < 3000) {
      const checkRes = await query<{ published_at: Date | null }>(
        `SELECT published_at FROM outbox WHERE id = $1;`,
        [outboxId]
      );
      if (checkRes.rows[0]?.published_at) {
        break;
      }
      await new Promise((r) => setTimeout(r, 100));
    }

    // 3. Verify published_at is set in database
    const verifySql = `SELECT published_at FROM outbox WHERE id = $1;`;
    const verifyRes = await query<{ published_at: Date | null }>(verifySql, [outboxId]);
    expect(verifyRes.rows[0]?.published_at).not.toBeNull();

    await publisher.stop();
  });

  it("handles 2 concurrent publisher instances without double-publishing (SKIP LOCKED)", async () => {
    // 1. Insert 10 rows
    const insertedSubIds: string[] = [];
    const insertedRowIds: string[] = [];

    for (let i = 0; i < 10; i++) {
      const subId = `22222222-3333-4444-5555-${(Date.now() + i).toString().slice(-12)}`;
      insertedSubIds.push(subId);

      const payload: SubmissionQueuedEvent = {
        submissionId: subId,
        problemId: "sum-two",
        language: "PYTHON",
        createdAt: new Date().toISOString(),
      };
      const res = await query<{ id: string }>(
        `INSERT INTO outbox (topic, msg_key, payload) VALUES ($1, $2, $3) RETURNING id;`,
        [TOPICS.SUBMISSIONS_QUEUED, subId, JSON.stringify(payload)]
      );
      insertedRowIds.push(res.rows[0]!.id);
    }

    // 2. Start two publishers concurrently
    const publisher1 = new OutboxPublisher({ clientId: "concurrent-pub-1", batchSize: 5 });
    const publisher2 = new OutboxPublisher({ clientId: "concurrent-pub-2", batchSize: 5 });

    await publisher1.start();
    await publisher2.start();

    // Wait until all 10 are marked published in the database
    const deadline = Date.now() + 5000;
    let publishedCount = 0;
    while (Date.now() < deadline) {
      const countRes = await query<{ count: string }>(
        `SELECT count(*) FROM outbox WHERE id = ANY($1::bigint[]) AND published_at IS NOT NULL;`,
        [insertedRowIds]
      );
      publishedCount = parseInt(countRes.rows[0]?.count ?? "0", 10);
      if (publishedCount === 10) break;
      await new Promise((r) => setTimeout(r, 100));
    }

    expect(publishedCount).toBe(10);

    // Wait a brief period for Kafka consumer to collect all messages
    await new Promise((r) => setTimeout(r, 1000));

    // Verify each of the 10 submission IDs appears in Kafka EXACTLY once
    for (const subId of insertedSubIds) {
      const occurrences = receivedKeys.filter((k) => k === subId).length;
      expect(occurrences).toBe(1);
    }

    await publisher1.stop();
    await publisher2.stop();
  });
});
