import { execSync } from "child_process";
import { pool, closeDb, createSubmissionClient } from "@bytearena/shared";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  console.log("=== Chaos Test 2: Outbox Publisher Crash & Drain Recovery ===");
  const submissionClient = createSubmissionClient("localhost:50051");

  let submissionId = "";
  let success = false;

  try {
    // 1. Stop the outbox publisher container
    console.log("[1/5] Stopping outbox-publisher service...");
    execSync("docker compose stop outbox-publisher", { stdio: "inherit" });

    // 2. Create submission while publisher is offline
    console.log("[2/5] Creating submission while outbox publisher is offline...");
    const createRes = await new Promise<{ submissionId: string; created: boolean }>(
      (resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: "sum-two",
            language: 1, // PYTHON
            code: "a, b = map(int, input().split())\nprint(a + b)",
            handle: "chaos-tester",
            idempotencyKey: `chaos-pub-${Date.now()}`,
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

    submissionId = createRes.submissionId;
    console.log(`[2/5] Submission created in DB: ${submissionId}`);

    // 3. Verify submission status is QUEUED and outbox row is unpublished
    const subCheck = await pool.query(
      "SELECT status FROM submissions WHERE id = $1",
      [submissionId]
    );
    const outboxCheck = await pool.query(
      "SELECT id, published_at FROM outbox WHERE payload->>'submissionId' = $1",
      [submissionId]
    );

    if (subCheck.rows[0]?.status !== "QUEUED") {
      throw new Error(`Expected status QUEUED, got: ${subCheck.rows[0]?.status}`);
    }
    if (outboxCheck.rows.length === 0 || outboxCheck.rows[0]?.published_at !== null) {
      throw new Error("Expected 1 outbox row with published_at IS NULL");
    }
    console.log(`[3/5] Verified outbox row #${outboxCheck.rows[0].id} is pending (published_at = null).`);

    // Wait 2 seconds to prove it does not get published while publisher is stopped
    await sleep(2000);
    const stillQueued = await pool.query(
      "SELECT status FROM submissions WHERE id = $1",
      [submissionId]
    );
    if (stillQueued.rows[0]?.status !== "QUEUED") {
      throw new Error("Submission changed state without outbox publisher!");
    }
    console.log("[3/5] Verified submission stayed QUEUED while publisher was down.");

    // 4. Restart outbox publisher
    console.log("[4/5] Starting outbox-publisher service...");
    execSync("docker compose start outbox-publisher", { stdio: "inherit" });

    // 5. Poll until completed
    console.log("[5/5] Waiting for publisher to drain outbox and runner to judge submission...");
    let completed = false;
    let finalStatus = "";
    let finalVerdict = "";

    const timeout = Date.now() + 30000;
    while (Date.now() < timeout) {
      const res = await pool.query(
        "SELECT status, verdict FROM submissions WHERE id = $1",
        [submissionId]
      );
      const row = res.rows[0];
      if (row?.status === "COMPLETED" || row?.status === "SYSTEM_ERROR") {
        completed = true;
        finalStatus = row.status;
        finalVerdict = row.verdict;
        break;
      }
      await sleep(500);
    }

    if (!completed) {
      throw new Error(`Timed out waiting for submission ${submissionId} to complete`);
    }

    // Verify outbox row now has published_at set
    const outboxFinal = await pool.query(
      "SELECT published_at FROM outbox WHERE payload->>'submissionId' = $1",
      [submissionId]
    );
    const publishedAt = outboxFinal.rows[0]?.published_at;

    const testCountRes = await pool.query(
      "SELECT count(*) FROM test_results WHERE submission_id = $1",
      [submissionId]
    );
    const totalTests = parseInt(testCountRes.rows[0].count, 10);

    console.log("----------------------------------------");
    console.log(`Outbox published_at: ${publishedAt}`);
    console.log(`Final Status:        ${finalStatus}`);
    console.log(`Final Verdict:       ${finalVerdict}`);
    console.log(`Test Results Rows:   ${totalTests}`);
    console.log("----------------------------------------");

    if (
      publishedAt &&
      finalStatus === "COMPLETED" &&
      finalVerdict === "ACCEPTED" &&
      totalTests === 5
    ) {
      console.log("PASS: Outbox publisher crash recovery verified.");
      success = true;
    } else {
      console.error("FAIL: State verification failed post-recovery");
      success = false;
    }
  } catch (err: unknown) {
    console.error("FAIL: Error during outbox publisher chaos test:", err);
    success = false;
  } finally {
    try {
      execSync("docker compose up -d outbox-publisher", { stdio: "ignore" });
    } catch {
      // ignore
    }
    await closeDb();
  }

  process.exit(success ? 0 : 1);
}

main();
