import {
  pool,
  closeDb,
  createSubmissionClient,
  createKafkaClient,
  createProducer,
  publishJsonMessage,
  TOPICS,
  SubmissionQueuedEvent,
  TestResultEvent,
  FinalVerdictEvent,
} from "@bytearena/shared";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  console.log("=== Chaos Test 4: Duplicate Message Delivery & Idempotency ===");
  const submissionClient = createSubmissionClient("localhost:50051");

  const kafka = createKafkaClient("chaos-duplicate-delivery");
  const producer = await createProducer(kafka);
  let success = false;

  try {
    // 1. Submit a valid solution
    console.log("[1/5] Submitting original solution...");
    const idempotencyKey = `chaos-dup-${Date.now()}`;
    const code = "a, b = map(int, input().split())\nprint(a + b)";

    const createRes = await new Promise<{ submissionId: string; created: boolean }>(
      (resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: "sum-two",
            language: 1, // PYTHON
            code,
            handle: "chaos-tester",
            idempotencyKey,
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
    console.log(`[1/5] Submission created: ${submissionId}`);

    // 2. Wait for it to complete normally
    console.log("[2/5] Waiting for submission to finish judging...");
    const timeout = Date.now() + 30000;
    let initialJudgedAt: string | null = null;

    while (Date.now() < timeout) {
      const res = await pool.query(
        "SELECT status, verdict, judged_at FROM submissions WHERE id = $1",
        [submissionId]
      );
      const row = res.rows[0];
      if (row?.status === "COMPLETED") {
        initialJudgedAt = row.judged_at;
        break;
      }
      await sleep(500);
    }

    if (!initialJudgedAt) {
      throw new Error(`Submission ${submissionId} did not reach COMPLETED status`);
    }

    const initialTestCountRes = await pool.query(
      "SELECT count(*) FROM test_results WHERE submission_id = $1",
      [submissionId]
    );
    const initialTestCount = parseInt(initialTestCountRes.rows[0].count, 10);
    console.log(
      `[2/5] Submission completed successfully. Initial test count: ${initialTestCount}`
    );

    // 3. Send duplicate SUBMISSION_QUEUED message to submissions.queued
    console.log("[3/5] Publishing duplicate SUBMISSION_QUEUED event directly to Kafka...");
    const duplicateQueuedEvent: SubmissionQueuedEvent = {
      type: "SUBMISSION_QUEUED",
      submissionId,
      problemId: "sum-two",
      language: "PYTHON",
      code,
      idempotencyKey,
      ts: new Date().toISOString(),
    };

    await publishJsonMessage({
      producer,
      topic: TOPICS.SUBMISSIONS_QUEUED,
      key: submissionId,
      value: duplicateQueuedEvent,
    });
    console.log("[3/5] Duplicate queued event published.");

    // 4. Send duplicate TEST_RESULT and FINAL_VERDICT to submissions.results
    console.log("[4/5] Publishing duplicate TEST_RESULT and FINAL_VERDICT events to Kafka...");
    const duplicateTestResult: TestResultEvent = {
      type: "TEST_RESULT",
      submissionId,
      testIndex: 1,
      verdict: "ACCEPTED",
      timeMs: 12,
      memoryKb: 1024,
      isSample: true,
      ts: new Date().toISOString(),
    };

    await publishJsonMessage({
      producer,
      topic: TOPICS.SUBMISSIONS_RESULTS,
      key: submissionId,
      value: duplicateTestResult,
    });

    const duplicateFinalVerdict: FinalVerdictEvent = {
      type: "FINAL_VERDICT",
      submissionId,
      verdict: "WRONG_ANSWER", // Intentionally contradictory to test first-final-verdict-wins immutability
      totalTests: 5,
      passedTests: 0,
      maxTimeMs: 100,
      maxMemoryKb: 2048,
      ts: new Date().toISOString(),
    };

    await publishJsonMessage({
      producer,
      topic: TOPICS.SUBMISSIONS_RESULTS,
      key: submissionId,
      value: duplicateFinalVerdict,
    });

    // Allow runner and result-writer time to process the duplicate events
    console.log("[4/5] Waiting 3 seconds for consumers to process duplicate events...");
    await sleep(3000);

    // 5. Verify PostgreSQL state remains identical (no extra rows, no overwritten verdict)
    console.log("[5/5] Verifying state immutability in PostgreSQL...");
    const finalSubRes = await pool.query(
      "SELECT id, status, verdict, judged_at FROM submissions WHERE id = $1",
      [submissionId]
    );
    const finalRow = finalSubRes.rows[0];

    const finalTestCountRes = await pool.query(
      "SELECT count(*) FROM test_results WHERE submission_id = $1",
      [submissionId]
    );
    const finalTestCount = parseInt(finalTestCountRes.rows[0].count, 10);

    const subRowCountRes = await pool.query(
      "SELECT count(*) FROM submissions WHERE id = $1",
      [submissionId]
    );
    const subRowCount = parseInt(subRowCountRes.rows[0].count, 10);

    console.log("----------------------------------------");
    console.log(`Submissions Rows:     ${subRowCount} (expected: 1)`);
    console.log(`Final Status:         ${finalRow?.status} (expected: COMPLETED)`);
    console.log(`Final Verdict:        ${finalRow?.verdict} (expected: ACCEPTED)`);
    console.log(`Test Results Rows:    ${finalTestCount} (expected: ${initialTestCount})`);
    console.log(`Judged Timestamp:     ${finalRow?.judged_at?.toISOString()}`);
    console.log("----------------------------------------");

    if (
      subRowCount === 1 &&
      finalRow?.status === "COMPLETED" &&
      finalRow?.verdict === "ACCEPTED" &&
      finalTestCount === initialTestCount
    ) {
      console.log("PASS: Duplicate delivery idempotency verified. Zero duplicates created, state unchanged.");
      success = true;
    } else {
      console.error("FAIL: State was mutated by duplicate messages");
      success = false;
    }
  } catch (err: unknown) {
    console.error("FAIL: Error during duplicate delivery chaos test:", err);
    success = false;
  } finally {
    await producer.disconnect();
    await closeDb();
  }

  process.exit(success ? 0 : 1);
}

main();
