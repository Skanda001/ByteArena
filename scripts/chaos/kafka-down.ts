import { execSync } from "child_process";
import * as net from "net";
import { pool, closeDb, createSubmissionClient } from "@bytearena/shared";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForKafka(host: string, port: number, timeoutMs = 60000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const isUp = await new Promise<boolean>((resolve) => {
      const sock = new net.Socket();
      sock.setTimeout(1000);
      sock.on("connect", () => {
        sock.destroy();
        resolve(true);
      });
      sock.on("error", () => {
        sock.destroy();
        resolve(false);
      });
      sock.on("timeout", () => {
        sock.destroy();
        resolve(false);
      });
      sock.connect(port, host);
    });

    if (isUp) {
      // Give Kafka broker an extra 2 seconds to complete metadata init
      await sleep(2000);
      return;
    }
    await sleep(500);
  }
  throw new Error(`Kafka did not become available at ${host}:${port} within ${timeoutMs}ms`);
}

async function main(): Promise<void> {
  console.log("=== Chaos Test 3: Kafka Outage Tolerance ===");
  const submissionClient = createSubmissionClient("localhost:50051");

  const subIds: string[] = [];
  let success = false;

  try {
    // 1. Stop Kafka container
    console.log("[1/5] Stopping Kafka container...");
    execSync("docker compose stop kafka", { stdio: "inherit" });

    // 2. Submit solutions while Kafka is down
    console.log("[2/5] Creating 2 submissions via gRPC while Kafka is offline...");

    const sub1Res = await new Promise<{ submissionId: string; created: boolean }>(
      (resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: "sum-two",
            language: 1, // PYTHON
            code: "a, b = map(int, input().split())\nprint(a + b)",
            handle: "chaos-tester",
            idempotencyKey: `chaos-kfk-1-${Date.now()}`,
          },
          (err, response) => {
            if (err) return reject(err);
            resolve({
              submissionId: response?.submission?.id ?? "",
              created: response?.created ?? false,
            });
          }
        );
      }
    );
    subIds.push(sub1Res.submissionId);
    console.log(`[2/5] Submission 1 created: ${sub1Res.submissionId}`);

    const sub2Res = await new Promise<{ submissionId: string; created: boolean }>(
      (resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: "reverse-string",
            language: 2, // JAVASCRIPT
            code: `const fs = require('fs');
const s = fs.readFileSync(0, 'utf-8').trim();
console.log(s.split('').reverse().join(''));`,
            handle: "chaos-tester",
            idempotencyKey: `chaos-kfk-2-${Date.now()}`,
          },
          (err, response) => {
            if (err) return reject(err);
            resolve({
              submissionId: response?.submission?.id ?? "",
              created: response?.created ?? false,
            });
          }
        );
      }
    );
    subIds.push(sub2Res.submissionId);
    console.log(`[2/5] Submission 2 created: ${sub2Res.submissionId}`);

    // 3. Verify both submissions exist in PostgreSQL and are pending in outbox
    console.log("[3/5] Verifying PostgreSQL state during Kafka outage...");
    for (const id of subIds) {
      const subCheck = await pool.query(
        "SELECT status FROM submissions WHERE id = $1",
        [id]
      );
      const outboxCheck = await pool.query(
        "SELECT published_at FROM outbox WHERE payload->>'submissionId' = $1",
        [id]
      );

      if (subCheck.rows[0]?.status !== "QUEUED") {
        throw new Error(`Expected submission ${id} to be QUEUED, got: ${subCheck.rows[0]?.status}`);
      }
      if (outboxCheck.rows.length === 0 || outboxCheck.rows[0]?.published_at !== null) {
        throw new Error(`Expected submission ${id} outbox published_at to be NULL`);
      }
    }
    console.log("[3/5] Verified: Submissions saved atomically; outbox rows pending.");

    // 4. Restart Kafka
    console.log("[4/5] Starting Kafka container...");
    execSync("docker compose start kafka", { stdio: "inherit" });
    console.log("[4/5] Waiting for Kafka to become available...");
    await waitForKafka("localhost", 29092, 45000);
    console.log("[4/5] Kafka is back online. Ensuring all consumer/publisher services are running...");
    execSync("docker compose up -d", { stdio: "inherit" });

    // 5. Wait for both submissions to complete
    console.log("[5/5] Waiting for outbox publisher to drain and runner to judge all submissions...");
    const timeout = Date.now() + 90000;

    for (const id of subIds) {
      let done = false;
      while (Date.now() < timeout) {
        const res = await pool.query(
          "SELECT status, verdict FROM submissions WHERE id = $1",
          [id]
        );
        const row = res.rows[0];
        if (row?.status === "COMPLETED" || row?.status === "SYSTEM_ERROR") {
          if (row.status !== "COMPLETED" || row.verdict !== "ACCEPTED") {
            throw new Error(`Submission ${id} failed with ${row.status} / ${row.verdict}`);
          }
          done = true;
          break;
        }
        await sleep(500);
      }
      if (!done) {
        throw new Error(`Timed out waiting for submission ${id} to complete`);
      }

      const outboxRes = await pool.query(
        "SELECT published_at FROM outbox WHERE payload->>'submissionId' = $1",
        [id]
      );
      if (!outboxRes.rows[0]?.published_at) {
        throw new Error(`Submission ${id} outbox row was not marked published_at`);
      }

      const testCountRes = await pool.query(
        "SELECT count(*) FROM test_results WHERE submission_id = $1",
        [id]
      );
      const testCount = parseInt(testCountRes.rows[0].count, 10);
      if (testCount !== 5) {
        throw new Error(`Submission ${id} expected 5 test results, got: ${testCount}`);
      }
      console.log(`[5/5] Submission ${id} verified: COMPLETED / ACCEPTED (5 test results).`);
    }

    console.log("----------------------------------------");
    console.log("All submissions queued during Kafka downtime were judged successfully.");
    console.log("----------------------------------------");
    console.log("PASS: Kafka downtime tolerance verified.");
    success = true;
  } catch (err: unknown) {
    console.error("FAIL: Error during Kafka downtime chaos test:", err);
    success = false;
  } finally {
    try {
      execSync("docker compose up -d", { stdio: "ignore" });
    } catch {
      // ignore
    }
    await closeDb();
  }

  process.exit(success ? 0 : 1);
}

main();
