import { describe, it, expect, afterAll } from "vitest";
import { query, closeDb } from "@bytearena/shared";
import { createSubmissionTx } from "./db";

describe("Submission Transaction and Idempotency", () => {
  afterAll(async () => {
    await closeDb();
  });

  it("proves a failure after submission insert rolls back both submission and outbox row", async () => {
    const handle = `rollback_user_${Date.now()}`;
    const idempotencyKey = `idem_rb_${Date.now()}`;

    // Expect transaction to reject due to simulated post-insert throw
    await expect(
      createSubmissionTx({
        problemId: "sum-two",
        language: "PYTHON",
        handle,
        code: "print(1 + 2)",
        idempotencyKey,
        simulateFailureAfterInsert: true,
      })
    ).rejects.toThrow("Simulated failure after submission insert for rollback test");

    // Verify NO submission row was persisted
    const subRes = await query<{ count: string }>(
      `SELECT count(*) FROM submissions WHERE handle = $1;`,
      [handle]
    );
    expect(parseInt(subRes.rows[0]?.count ?? "0", 10)).toBe(0);

    // Verify NO outbox row was persisted
    const outboxRes = await query<{ count: string }>(
      `SELECT count(*) FROM outbox WHERE payload->>'handle' = $1;`,
      [handle]
    );
    expect(parseInt(outboxRes.rows[0]?.count ?? "0", 10)).toBe(0);
  });

  it("handles idempotent submission correctly and avoids duplicate outbox rows", async () => {
    const handle = `idempotent_user_${Date.now()}`;
    const idempotencyKey = `idem_test_${Date.now()}`;

    // 1. First creation
    const firstRes = await createSubmissionTx({
      problemId: "sum-two",
      language: "PYTHON",
      handle,
      code: "print(1 + 2)",
      idempotencyKey,
    });

    expect(firstRes.created).toBe(true);
    const subId = firstRes.submission.id;
    expect(subId).toBeDefined();

    // Verify 1 outbox row
    const outboxRes1 = await query<{ count: string }>(
      `SELECT count(*) FROM outbox WHERE msg_key = $1;`,
      [subId]
    );
    expect(parseInt(outboxRes1.rows[0]?.count ?? "0", 10)).toBe(1);

    // 2. Second creation with identical handle and idempotency key
    const secondRes = await createSubmissionTx({
      problemId: "sum-two",
      language: "PYTHON",
      handle,
      code: "print(1 + 2)",
      idempotencyKey,
    });

    expect(secondRes.created).toBe(false);
    expect(secondRes.submission.id).toBe(subId);

    // Verify STILL only 1 outbox row and 1 submission row
    const outboxRes2 = await query<{ count: string }>(
      `SELECT count(*) FROM outbox WHERE msg_key = $1;`,
      [subId]
    );
    expect(parseInt(outboxRes2.rows[0]?.count ?? "0", 10)).toBe(1);

    const subRes = await query<{ count: string }>(
      `SELECT count(*) FROM submissions WHERE handle = $1 AND idempotency_key = $2;`,
      [handle, idempotencyKey]
    );
    expect(parseInt(subRes.rows[0]?.count ?? "0", 10)).toBe(1);
  });
});
