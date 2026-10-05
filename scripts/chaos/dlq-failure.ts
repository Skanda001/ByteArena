import { execSync } from "child_process";
import Docker from "dockerode";
import {
  pool,
  closeDb,
  createSubmissionClient,
  createKafkaClient,
  createConsumer,
  TOPICS,
  SubmissionDlqEventSchema,
  SubmissionDlqEvent,
} from "@bytearena/shared";
import { RunnerWorker } from "../../services/runner/src/worker";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  console.log("=== Chaos Test 5: Infrastructure Failure & DLQ Routing ===");
  const submissionClient = createSubmissionClient("localhost:50051");

  let worker: RunnerWorker | null = null;
  const kafka = createKafkaClient("dlq-verifier");
  const dlqConsumer = await createConsumer(kafka, {
    groupId: `dlq-verifier-group-${Date.now()}`,
  });
  let success = false;

  try {
    // 1. Stop healthy runner service so our broken worker consumes the test submission
    console.log("[1/6] Stopping healthy runner container...");
    execSync("docker compose stop runner", { stdio: "inherit" });

    // 2. Subscribe to submissions.dlq
    console.log("[2/6] Subscribing to submissions.dlq topic...");
    await dlqConsumer.subscribe({
      topic: TOPICS.SUBMISSIONS_DLQ,
      fromBeginning: false,
    });

    const dlqEvents: SubmissionDlqEvent[] = [];
    await dlqConsumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        try {
          const raw = JSON.parse(message.value.toString("utf-8"));
          const event = SubmissionDlqEventSchema.parse(raw);
          dlqEvents.push(event);
        } catch {
          // ignore
        }
      },
    });

    // 3. Start a test RunnerWorker configured with an unreachable Docker daemon
    console.log("[3/6] Starting test RunnerWorker with unreachable Docker daemon...");
    const unreachableDocker = new Docker({ host: "127.0.0.1", port: 19999, timeout: 2000 });
    worker = new RunnerWorker({
      docker: unreachableDocker,
      groupId: "runner-group",
      clientId: "chaos-dlq-runner",
    });

    await worker.start();

    // 4. Create submission via gRPC
    console.log("[4/6] Creating submission that will encounter Docker daemon failure...");
    const createRes = await new Promise<{ submissionId: string; created: boolean }>(
      (resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: "sum-two",
            language: 1, // PYTHON
            code: "a, b = map(int, input().split())\nprint(a + b)",
            handle: "chaos-dlq-tester",
            idempotencyKey: `chaos-dlq-${Date.now()}`,
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

    const submissionId = createRes.submissionId;
    console.log(`[4/6] Submission created: ${submissionId}`);

    // 5. Wait for worker to retry 3 times and route to DLQ
    console.log("[5/6] Waiting for 3 retry attempts and DLQ routing (~6-10s)...");
    const timeout = Date.now() + 45000;
    let dlqReceived = false;
    let dlqEventMatch: SubmissionDlqEvent | null = null;

    while (Date.now() < timeout) {
      const match = dlqEvents.find((e) => e.submissionId === submissionId);
      if (match) {
        dlqReceived = true;
        dlqEventMatch = match;
        break;
      }
      await sleep(500);
    }

    if (!dlqReceived || !dlqEventMatch) {
      throw new Error(`Did not receive DLQ event for submission ${submissionId}`);
    }
    console.log(`[5/6] Received DLQ event with reason: "${dlqEventMatch.reason}", attempts: ${dlqEventMatch.attempts}`);

    // 6. Verify PostgreSQL state: status = SYSTEM_ERROR, verdict = INTERNAL_ERROR
    console.log("[6/6] Verifying submission final status in PostgreSQL...");
    let finalStatus = "";
    let finalVerdict = "";

    const subTimeout = Date.now() + 15000;
    while (Date.now() < subTimeout) {
      const res = await pool.query(
        "SELECT status, verdict FROM submissions WHERE id = $1",
        [submissionId]
      );
      const row = res.rows[0];
      if (row?.status === "SYSTEM_ERROR" || row?.status === "COMPLETED") {
        finalStatus = row.status;
        finalVerdict = row.verdict;
        break;
      }
      await sleep(500);
    }

    console.log("----------------------------------------");
    console.log(`DLQ Submission ID: ${dlqEventMatch.submissionId}`);
    console.log(`DLQ Attempts:      ${dlqEventMatch.attempts} (expected: 3)`);
    console.log(`Final Status:      ${finalStatus} (expected: SYSTEM_ERROR)`);
    console.log(`Final Verdict:     ${finalVerdict} (expected: INTERNAL_ERROR)`);
    console.log("----------------------------------------");

    if (
      dlqEventMatch.attempts === 3 &&
      finalStatus === "SYSTEM_ERROR" &&
      finalVerdict === "INTERNAL_ERROR"
    ) {
      console.log(
        "PASS: DLQ path verified. Infrastructure failure retried 3 times, routed to submissions.dlq, and marked SYSTEM_ERROR."
      );
      success = true;
    } else {
      console.error("FAIL: DLQ verification failed");
      success = false;
    }
  } catch (err: unknown) {
    console.error("FAIL: Error during DLQ chaos test:", err);
    success = false;
  } finally {
    if (worker) {
      try {
        await worker.stop();
      } catch {
        // ignore
      }
    }
    try {
      await dlqConsumer.disconnect();
    } catch {
      // ignore
    }
    try {
      console.log("[cleanup] Restoring runner container...");
      execSync("docker compose up -d runner", { stdio: "inherit" });
      await sleep(1000);
    } catch {
      // ignore
    }
    await closeDb();
  }

  process.exit(success ? 0 : 1);
}

main();
