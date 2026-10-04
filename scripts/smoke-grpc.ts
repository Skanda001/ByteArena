import {
  createSubmissionClient,
  createJudgeClient,
  query,
  closeDb,
  config,
} from "@bytearena/shared";
import { startServer, stopServer } from "../services/submission-service/src/server";

async function runSmokeTest() {
  console.log("=== Starting Phase 1 Smoke Test ===");

  // 1. Start gRPC server
  console.log(`Starting gRPC server on port ${config.GRPC_PORT}...`);
  const _server = await startServer(config.GRPC_PORT);

  const submissionClient = createSubmissionClient(`localhost:${config.GRPC_PORT}`);
  const judgeClient = createJudgeClient(`localhost:${config.GRPC_PORT}`);

  try {
    const testHandle = `smoke_user_${Date.now()}`;
    const idempotencyKey = `idem_${Date.now()}`;
    const testCode = "a, b = map(int, input().split())\nprint(a + b)\n";

    // 2. Call CreateSubmission first time
    console.log(`Calling CreateSubmission (handle: ${testHandle}, key: ${idempotencyKey})...`);
    const firstResponse = await new Promise<{
      submission: { id: string; handle: string; status: string; verdict: string };
      created: boolean;
    }>((resolve, reject) => {
      submissionClient.CreateSubmission(
        {
          problemId: "sum-two",
          language: "LANGUAGE_PYTHON" as unknown as number,
          code: testCode,
          handle: testHandle,
          idempotencyKey: idempotencyKey,
        },
        (err, res) => {
          if (err) return reject(err);
          if (!res) return reject(new Error("No response received"));
          resolve(
            res as unknown as {
              submission: { id: string; handle: string; status: string; verdict: string };
              created: boolean;
            }
          );
        }
      );
    });

    const submissionId = firstResponse.submission.id;
    console.log(`First call succeeded: id=${submissionId}, created=${firstResponse.created}`);

    if (!firstResponse.created) {
      throw new Error("Expected first CreateSubmission to return created=true");
    }

    // 3. Verify outbox has 1 unpublished row for this submission
    const outboxRes1 = await query<{ count: string }>(
      `SELECT count(*) FROM outbox WHERE published_at IS NULL AND msg_key = $1;`,
      [submissionId]
    );
    const outboxCount1 = parseInt(outboxRes1.rows[0]?.count ?? "0", 10);
    console.log(`Unpublished outbox rows for ${submissionId}: ${outboxCount1}`);
    if (outboxCount1 !== 1) {
      throw new Error(`Expected exactly 1 outbox row, found ${outboxCount1}`);
    }

    // 4. Verify submissions table has 1 row
    const subRes1 = await query<{ count: string }>(
      `SELECT count(*) FROM submissions WHERE id = $1;`,
      [submissionId]
    );
    const subCount1 = parseInt(subRes1.rows[0]?.count ?? "0", 10);
    console.log(`Submissions table rows for ${submissionId}: ${subCount1}`);
    if (subCount1 !== 1) {
      throw new Error(`Expected exactly 1 submission row, found ${subCount1}`);
    }

    // 5. Call CreateSubmission second time with same handle + idempotency key
    console.log(
      "Calling CreateSubmission second time with identical handle and idempotency key..."
    );
    const secondResponse = await new Promise<{
      submission: { id: string };
      created: boolean;
    }>((resolve, reject) => {
      submissionClient.CreateSubmission(
        {
          problemId: "sum-two",
          language: "LANGUAGE_PYTHON" as unknown as number,
          code: testCode,
          handle: testHandle,
          idempotencyKey: idempotencyKey,
        },
        (err, res) => {
          if (err) return reject(err);
          if (!res) return reject(new Error("No response received"));
          resolve(res as unknown as { submission: { id: string }; created: boolean });
        }
      );
    });

    console.log(
      `Second call succeeded: id=${secondResponse.submission.id}, created=${secondResponse.created}`
    );

    if (secondResponse.created !== false) {
      throw new Error(
        "Expected second CreateSubmission to return created=false (idempotent replay)"
      );
    }

    if (secondResponse.submission.id !== submissionId) {
      throw new Error(
        `Expected same submission id (${submissionId}), but got ${secondResponse.submission.id}`
      );
    }

    // 6. Verify still only 1 outbox row and 1 submission row
    const outboxRes2 = await query<{ count: string }>(
      `SELECT count(*) FROM outbox WHERE published_at IS NULL AND msg_key = $1;`,
      [submissionId]
    );
    const outboxCount2 = parseInt(outboxRes2.rows[0]?.count ?? "0", 10);
    console.log(`Unpublished outbox rows after replay: ${outboxCount2}`);
    if (outboxCount2 !== 1) {
      throw new Error(`Expected still exactly 1 outbox row after replay, found ${outboxCount2}`);
    }

    const subRes2 = await query<{ count: string }>(
      `SELECT count(*) FROM submissions WHERE handle = $1 AND idempotency_key = $2;`,
      [testHandle, idempotencyKey]
    );
    const subCount2 = parseInt(subRes2.rows[0]?.count ?? "0", 10);
    console.log(`Submission rows with this (handle, key): ${subCount2}`);
    if (subCount2 !== 1) {
      throw new Error(`Expected still exactly 1 submission row after replay, found ${subCount2}`);
    }

    // 7. Verify JudgeService.GetJudgingJob
    console.log(`Calling JudgeService.GetJudgingJob for submission ${submissionId}...`);
    const jobResponse = await new Promise<{
      alreadyFinal: boolean;
      submissionId: string;
      testCases: { testIndex: number; input: string; isSample: boolean }[];
    }>((resolve, reject) => {
      judgeClient.GetJudgingJob({ submissionId: submissionId }, (err, res) => {
        if (err) return reject(err);
        if (!res) return reject(new Error("No judging job response received"));
        resolve(
          res as unknown as {
            alreadyFinal: boolean;
            submissionId: string;
            testCases: { testIndex: number; input: string; isSample: boolean }[];
          }
        );
      });
    });

    console.log(
      `GetJudgingJob response: alreadyFinal=${jobResponse.alreadyFinal}, testCases=${jobResponse.testCases?.length}`
    );
    if (jobResponse.alreadyFinal !== false) {
      throw new Error("Expected alreadyFinal to be false for new submission");
    }
    if (!jobResponse.testCases || jobResponse.testCases.length !== 5) {
      throw new Error(`Expected 5 test cases for sum-two, got ${jobResponse.testCases?.length}`);
    }

    // 8. Verify ListProblems and samples only
    const problemsResponse = await new Promise<{
      problems: { id: string; samples: { input: string }[] }[];
    }>((resolve, reject) => {
      submissionClient.ListProblems({}, (err, res) => {
        if (err) return reject(err);
        resolve(res as unknown as { problems: { id: string; samples: { input: string }[] }[] });
      });
    });

    console.log(`ListProblems returned ${problemsResponse.problems?.length} problems`);
    const sumTwoProblem = problemsResponse.problems.find((p) => p.id === "sum-two");
    if (!sumTwoProblem || sumTwoProblem.samples.length !== 2) {
      throw new Error(
        `Expected sum-two to have exactly 2 sample cases, got ${sumTwoProblem?.samples?.length}`
      );
    }

    console.log("\n>>> ALL PHASE 1 SMOKE CHECKS PASSED SUCCESSFULLY! <<<\n");
  } finally {
    submissionClient.close();
    judgeClient.close();
    await stopServer();
    await closeDb();
  }
}

runSmokeTest().catch((err) => {
  console.error("Phase 1 smoke test FAILED:", err);
  process.exit(1);
});
