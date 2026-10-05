import { execSync } from "child_process";
import { pool, closeDb, createSubmissionClient } from "@bytearena/shared";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  console.log("=== Chaos Test 1: Runner Kill & Mid-Judging Recovery ===");
  const submissionClient = createSubmissionClient("localhost:50051");

  let submissionId = "";
  let recoveryTimeMs = 0;
  let success = false;

  try {
    // 1. Submit a valid solution with slight delay per test so it is easy to catch mid-judging
    const code = `import time
time.sleep(0.4)
a, b = map(int, input().split())
print(a + b)
`;

    const createRes = await new Promise<{ submissionId: string; created: boolean }>(
      (resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: "sum-two",
            language: 1, // PYTHON
            code,
            handle: "chaos-tester",
            idempotencyKey: `chaos-runner-${Date.now()}`,
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
    console.log(`[1/5] Submission created: ${submissionId}`);

    // 2. Poll DB until status is JUDGING
    console.log("[2/5] Waiting for runner to pick up submission and enter JUDGING...");
    let inJudging = false;
    for (let i = 0; i < 150; i++) {
      const res = await pool.query(
        "SELECT status FROM submissions WHERE id = $1",
        [submissionId]
      );
      if (res.rows[0]?.status === "JUDGING") {
        inJudging = true;
        break;
      }
      await sleep(200);
    }

    if (!inJudging) {
      throw new Error("Submission never entered JUDGING state");
    }

    // Wait ~400ms for 1-2 tests to complete
    await sleep(400);

    // Check partial tests
    const partialRes = await pool.query(
      "SELECT count(*) FROM test_results WHERE submission_id = $1",
      [submissionId]
    );
    const partialCount = parseInt(partialRes.rows[0].count, 10);
    console.log(`[3/5] Mid-judging reached with ${partialCount} partial tests written. Killing runner...`);

    // 3. Kill the runner container abruptly (SIGKILL)
    const tKill = Date.now();
    execSync("docker compose kill runner", { stdio: "inherit" });
    console.log("[3/5] Runner killed via SIGKILL. Verifying offset was NOT committed...");

    // Brief pause to let Kafka notice if needed
    await sleep(1000);

    // 4. Restart runner
    console.log("[4/5] Restarting runner container...");
    execSync("docker compose start runner", { stdio: "inherit" });

    // 5. Poll until COMPLETED
    console.log("[5/5] Waiting for submission to finish recovery and complete...");
    let completed = false;
    let finalStatus = "";
    let finalVerdict = "";

    const timeout = Date.now() + 60000; // 60s max
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
        recoveryTimeMs = Date.now() - tKill;
        break;
      }
      await sleep(500);
    }

    if (!completed) {
      throw new Error(`Timed out waiting for submission ${submissionId} to complete`);
    }

    // Verify exactly 5 test results and 1 submission row
    const testCountRes = await pool.query(
      "SELECT count(*) FROM test_results WHERE submission_id = $1",
      [submissionId]
    );
    const totalTests = parseInt(testCountRes.rows[0].count, 10);

    const subCountRes = await pool.query(
      "SELECT count(*) FROM submissions WHERE id = $1",
      [submissionId]
    );
    const subCount = parseInt(subCountRes.rows[0].count, 10);

    console.log("----------------------------------------");
    console.log(`Final Status:      ${finalStatus}`);
    console.log(`Final Verdict:     ${finalVerdict}`);
    console.log(`Submissions Rows:  ${subCount}`);
    console.log(`Test Results Rows: ${totalTests}`);
    console.log(`Recovery Time:     ${recoveryTimeMs}ms`);
    console.log("----------------------------------------");

    if (
      finalStatus === "COMPLETED" &&
      finalVerdict === "ACCEPTED" &&
      subCount === 1 &&
      totalTests === 5
    ) {
      console.log(`PASS: Runner kill recovery verified. Recovery time: ${recoveryTimeMs}ms`);
      success = true;
    } else {
      console.error("FAIL: State verification failed post-recovery");
      success = false;
    }
  } catch (err: unknown) {
    console.error("FAIL: Error during runner kill chaos test:", err);
    success = false;
  } finally {
    // Ensure runner is running
    try {
      execSync("docker compose up -d runner", { stdio: "ignore" });
    } catch {
      // ignore
    }
    await closeDb();
  }

  process.exit(success ? 0 : 1);
}

main();
