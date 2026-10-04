import { execSync } from "child_process";
import {
  query,
  closeDb,
  TOPICS,
  createKafkaClient,
  createConsumer,
} from "@bytearena/shared";
import { createSubmissionTx } from "../services/submission-service/src/db";
import { OutboxPublisher } from "../services/submission-service/src/outbox-publisher";

async function runCrashTest() {
  console.log("=== Starting Phase 2 Outbox Crash Test ===");

  // 1. Start OutboxPublisher in the background
  console.log("Starting OutboxPublisher...");
  const publisher = new OutboxPublisher({
    clientId: "crash-test-publisher",
    pollIntervalMs: 200,
  });
  await publisher.start();

  try {
    // 2. Stop Kafka container
    console.log("Stopping Kafka container via 'docker compose stop kafka'...");
    execSync("docker compose stop kafka", { stdio: "inherit" });
    console.log("Kafka container is stopped.");

    // 3. Create 3 submissions while Kafka is down
    console.log("Creating 3 submissions while Kafka is down...");
    const createdSubIds: string[] = [];
    for (let i = 1; i <= 3; i++) {
      const handle = `crash_user_${Date.now()}_${i}`;
      const idempotencyKey = `idem_crash_${Date.now()}_${i}`;
      const res = await createSubmissionTx({
        problemId: "sum-two",
        language: "PYTHON",
        handle,
        code: `print(${i} + ${i})`,
        idempotencyKey,
      });

      console.log(`Created submission ${i}: id=${res.submission.id}, handle=${handle}`);
      createdSubIds.push(res.submission.id);
    }

    // 4. Verify outbox table grew and rows remain unpublished
    const pendingRes = await query<{ count: string }>(
      `SELECT count(*) FROM outbox WHERE msg_key = ANY($1::text[]) AND published_at IS NULL;`,
      [createdSubIds]
    );
    const pendingCount = parseInt(pendingRes.rows[0]?.count ?? "0", 10);
    console.log(`Unpublished outbox rows while Kafka is down: ${pendingCount} (expected: 3)`);
    if (pendingCount !== 3) {
      throw new Error(`Expected 3 unpublished rows, found ${pendingCount}`);
    }

    // 5. Start Kafka container back up
    console.log("Starting Kafka container back up via 'docker compose start kafka'...");
    execSync("docker compose start kafka", { stdio: "inherit" });
    console.log("Kafka container started. Waiting for Kafka to become ready...");

    // Wait for Kafka broker to accept connections
    let kafkaReady = false;
    const maxWaitTime = 40000;
    const startWait = Date.now();
    while (Date.now() - startWait < maxWaitTime) {
      try {
        const output = execSync(
          "docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092",
          { stdio: "pipe" }
        ).toString();
        if (output.includes("id: 1")) {
          kafkaReady = true;
          break;
        }
      } catch {
        // waiting
      }
      await new Promise((r) => setTimeout(r, 2000));
    }

    if (!kafkaReady) {
      throw new Error("Kafka did not become ready within timeout");
    }
    console.log("Kafka broker is healthy and responding!");

    // 6. Wait for OutboxPublisher to automatically drain outbox rows
    console.log("Waiting for OutboxPublisher to publish pending outbox rows...");
    let allPublished = false;
    const drainDeadline = Date.now() + 30000;
    while (Date.now() < drainDeadline) {
      const checkRes = await query<{ count: string }>(
        `SELECT count(*) FROM outbox WHERE msg_key = ANY($1::text[]) AND published_at IS NOT NULL;`,
        [createdSubIds]
      );
      const published = parseInt(checkRes.rows[0]?.count ?? "0", 10);
      if (published === 3) {
        allPublished = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 1000));
    }

    if (!allPublished) {
      throw new Error("Outbox rows were not published within deadline");
    }
    console.log("All 3 outbox rows are now marked published_at in the database!");

    // 7. Verify all 3 messages are received from Kafka
    console.log("Verifying messages received from Kafka topic submissions.queued...");
    const receivedFromKafka = new Set<string>();
    const kafkaClient = createKafkaClient("crash-test-consumer");
    const consumer = await createConsumer(kafkaClient, {
      groupId: `crash-test-verify-${Date.now()}`,
    });

    await consumer.subscribe({
      topic: TOPICS.SUBMISSIONS_QUEUED,
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (message.key) {
          const keyStr = message.key.toString();
          if (createdSubIds.includes(keyStr)) {
            receivedFromKafka.add(keyStr);
          }
        }
      },
    });

    const consumerDeadline = Date.now() + 15000;
    while (Date.now() < consumerDeadline && receivedFromKafka.size < 3) {
      await new Promise((r) => setTimeout(r, 500));
    }

    await consumer.disconnect();

    console.log(`Received ${receivedFromKafka.size}/3 created submissions from Kafka.`);
    if (receivedFromKafka.size !== 3) {
      throw new Error(
        `Expected all 3 messages in Kafka, but only received ${receivedFromKafka.size}`
      );
    }

    console.log("\n>>> CRASH TEST PASSED: No messages lost, all 3 arrived! <<<\n");
  } finally {
    await publisher.stop();
    await closeDb();
  }
}

runCrashTest().catch((err) => {
  console.error("Crash test FAILED:", err);
  process.exit(1);
});
