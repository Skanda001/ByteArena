import { describe, it, expect, afterAll } from "vitest";
import { ResultWriter } from "./result-writer";
import { pool, getClient } from "@bytearena/shared";

// Integration test — requires a running Postgres and Kafka.
// Run via: npx vitest run services/submission-service/src/result-writer.test.ts

describe("ResultWriter idempotency", () => {
  const SUB_ID = "00000000-0000-0000-0000-000000000099";
  const writer = new ResultWriter({ closeDbOnStop: false });

  afterAll(async () => {
    await writer.stop();
    // Clean up test submission
    await pool.query("DELETE FROM submissions WHERE id = $1", [SUB_ID]);
  });

  async function seedSubmission(): Promise<void> {
    await pool.query(`
      INSERT INTO submissions (id, problem_id, language, handle, code, status)
      VALUES ($1, 'sum-two', 'PYTHON', 'test-writer', 'print(1)', 'QUEUED')
      ON CONFLICT (id) DO NOTHING
    `, [SUB_ID]);
  }

  it("handles JUDGING_STARTED: transitions QUEUED→JUDGING, repeated call is a no-op", async () => {
    await seedSubmission();
    const client = await getClient();
    try {
      // First call — should transition
      await (writer as unknown as { handleJudgingStarted: (raw: unknown) => Promise<void> })
        ["handleJudgingStarted"]({
          type: "JUDGING_STARTED",
          submissionId: SUB_ID,
          totalTests: 5,
          ts: new Date().toISOString(),
        });

      const r1 = await client.query<{ status: string }>(
        "SELECT status FROM submissions WHERE id = $1",
        [SUB_ID]
      );
      expect(r1.rows[0]?.status).toBe("JUDGING");

      // Second call — should be a no-op (still JUDGING)
      await (writer as unknown as { handleJudgingStarted: (raw: unknown) => Promise<void> })
        ["handleJudgingStarted"]({
          type: "JUDGING_STARTED",
          submissionId: SUB_ID,
          totalTests: 5,
          ts: new Date().toISOString(),
        });

      const r2 = await client.query<{ status: string }>(
        "SELECT status FROM submissions WHERE id = $1",
        [SUB_ID]
      );
      expect(r2.rows[0]?.status).toBe("JUDGING");
    } finally {
      client.release();
    }
  });

  it("handles TEST_RESULT: inserts a row, duplicate is silently ignored", async () => {
    await seedSubmission();
    const ev = {
      type: "TEST_RESULT" as const,
      submissionId: SUB_ID,
      testIndex: 1,
      verdict: "ACCEPTED" as const,
      timeMs: 42,
      memoryKb: 0,
      isSample: true,
      ts: new Date().toISOString(),
    };

    // First insert
    await (writer as unknown as { handleTestResult: (raw: unknown) => Promise<void> })
      ["handleTestResult"](ev);

    const r1 = await pool.query<{ verdict: string }>(
      "SELECT verdict FROM test_results WHERE submission_id = $1 AND test_index = 1",
      [SUB_ID]
    );
    expect(r1.rows).toHaveLength(1);
    expect(r1.rows[0]?.verdict).toBe("ACCEPTED");

    // Duplicate — must not throw, must remain exactly 1 row
    await (writer as unknown as { handleTestResult: (raw: unknown) => Promise<void> })
      ["handleTestResult"](ev);

    const r2 = await pool.query(
      "SELECT count(*) FROM test_results WHERE submission_id = $1 AND test_index = 1",
      [SUB_ID]
    );
    expect(Number(r2.rows[0]?.count)).toBe(1);
  });

  it("handles FINAL_VERDICT: transitions QUEUED/JUDGING→COMPLETED, repeated call is a no-op", async () => {
    await seedSubmission();
    const ev = {
      type: "FINAL_VERDICT" as const,
      submissionId: SUB_ID,
      verdict: "ACCEPTED" as const,
      ts: new Date().toISOString(),
    };

    // First call — should set COMPLETED/ACCEPTED
    await (writer as unknown as { handleFinalVerdict: (raw: unknown) => Promise<void> })
      ["handleFinalVerdict"](ev);

    const r1 = await pool.query<{ status: string; verdict: string }>(
      "SELECT status, verdict FROM submissions WHERE id = $1",
      [SUB_ID]
    );
    expect(r1.rows[0]?.status).toBe("COMPLETED");
    expect(r1.rows[0]?.verdict).toBe("ACCEPTED");

    // Second call — no-op, state unchanged
    await (writer as unknown as { handleFinalVerdict: (raw: unknown) => Promise<void> })
      ["handleFinalVerdict"]({ ...ev, verdict: "WRONG_ANSWER" });

    const r2 = await pool.query<{ verdict: string }>(
      "SELECT verdict FROM submissions WHERE id = $1",
      [SUB_ID]
    );
    // Must still be ACCEPTED — first writer wins
    expect(r2.rows[0]?.verdict).toBe("ACCEPTED");
  });

  it("handles INTERNAL_ERROR verdict: sets status=SYSTEM_ERROR", async () => {
    // Seed a fresh submission
    const id2 = "00000000-0000-0000-0000-000000000098";
    await pool.query(`
      INSERT INTO submissions (id, problem_id, language, handle, code, status)
      VALUES ($1, 'sum-two', 'PYTHON', 'test-writer2', 'print(1)', 'JUDGING')
      ON CONFLICT (id) DO NOTHING
    `, [id2]);

    await (writer as unknown as { handleFinalVerdict: (raw: unknown) => Promise<void> })
      ["handleFinalVerdict"]({
        type: "FINAL_VERDICT",
        submissionId: id2,
        verdict: "INTERNAL_ERROR",
        ts: new Date().toISOString(),
      });

    const r = await pool.query<{ status: string }>(
      "SELECT status FROM submissions WHERE id = $1",
      [id2]
    );
    expect(r.rows[0]?.status).toBe("SYSTEM_ERROR");

    // Cleanup
    await pool.query("DELETE FROM submissions WHERE id = $1", [id2]);
  });
});
