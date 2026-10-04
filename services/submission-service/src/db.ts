import {
  getClient,
  query,
  toProtoLanguage,
  toProtoStatus,
  toProtoVerdict,
  LanguageType,
  SubmissionStatusType,
  VerdictType,
  TOPICS,
  SubmissionQueuedEvent,
} from "@bytearena/shared";
import type { Submission } from "@bytearena/shared";
import type { Problem } from "@bytearena/shared";
import type { JudgeTestCase } from "@bytearena/shared";

export interface SubmissionRow {
  id: string;
  problem_id: string;
  language: string;
  handle: string;
  code: string;
  status: string;
  verdict: string | null;
  idempotency_key: string | null;
  created_at: Date;
  judged_at: Date | null;
}

export interface TestResultRow {
  submission_id?: string;
  test_index: number;
  verdict: string;
  time_ms: number;
  memory_kb: number;
  is_sample: boolean;
}

export interface ProblemRow {
  id: string;
  title: string;
  statement: string;
  time_limit_ms: number;
  memory_limit_mb: number;
}

export interface TestCaseRow {
  problem_id: string;
  test_index: number;
  input: string;
  expected_output: string;
  is_sample: boolean;
}

export function formatSubmission(row: SubmissionRow, results: TestResultRow[] = []): Submission {
  return {
    id: row.id,
    problemId: row.problem_id,
    language: toProtoLanguage(row.language as LanguageType),
    handle: row.handle,
    status: toProtoStatus(row.status as SubmissionStatusType),
    verdict: toProtoVerdict(row.verdict as VerdictType | null),
    createdAt: new Date(row.created_at).toISOString(),
    judgedAt: row.judged_at ? new Date(row.judged_at).toISOString() : "",
    results: results.map((r) => ({
      testIndex: r.test_index,
      verdict: toProtoVerdict(r.verdict as VerdictType),
      timeMs: r.time_ms ?? 0,
      memoryKb: r.memory_kb ?? 0,
      isSample: r.is_sample,
    })),
  };
}

export interface CreateSubmissionResult {
  submission: Submission;
  created: boolean;
}

export async function createSubmissionTx(params: {
  problemId: string;
  language: LanguageType;
  handle: string;
  code: string;
  idempotencyKey?: string | null;
  simulateFailureAfterInsert?: boolean; // For testing transactional rollback
}): Promise<CreateSubmissionResult> {
  const client = await getClient();
  try {
    await client.query("BEGIN");

    let submissionRow: SubmissionRow | null = null;
    let isCreated = false;

    if (params.idempotencyKey) {
      // Idempotent insertion using partial unique index
      const insertSql = `
        INSERT INTO submissions (problem_id, language, handle, code, idempotency_key)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (handle, idempotency_key) WHERE idempotency_key IS NOT NULL
        DO NOTHING
        RETURNING id, problem_id, language, handle, code, status, verdict, idempotency_key, created_at, judged_at;
      `;
      const res = await client.query<SubmissionRow>(insertSql, [
        params.problemId,
        params.language,
        params.handle,
        params.code,
        params.idempotencyKey,
      ]);

      if (res.rowCount && res.rows[0]) {
        submissionRow = res.rows[0];
        isCreated = true;
      } else {
        // Conflict occurred: fetch existing submission
        const selectSql = `
          SELECT id, problem_id, language, handle, code, status, verdict, idempotency_key, created_at, judged_at
          FROM submissions
          WHERE handle = $1 AND idempotency_key = $2;
        `;
        const existing = await client.query<SubmissionRow>(selectSql, [
          params.handle,
          params.idempotencyKey,
        ]);
        if (!existing.rows[0]) {
          throw new Error("Concurrent modification during idempotent submission create");
        }
        submissionRow = existing.rows[0];
        isCreated = false;
      }
    } else {
      const insertSql = `
        INSERT INTO submissions (problem_id, language, handle, code)
        VALUES ($1, $2, $3, $4)
        RETURNING id, problem_id, language, handle, code, status, verdict, idempotency_key, created_at, judged_at;
      `;
      const res = await client.query<SubmissionRow>(insertSql, [
        params.problemId,
        params.language,
        params.handle,
        params.code,
      ]);
      submissionRow = res.rows[0]!;
      isCreated = true;
    }

    if (params.simulateFailureAfterInsert) {
      throw new Error("Simulated failure after submission insert for rollback test");
    }

    // Only if created: insert outbox row in the same transaction
    if (isCreated) {
      const queuedEvent: SubmissionQueuedEvent = {
        submissionId: submissionRow.id,
        problemId: submissionRow.problem_id,
        language: params.language,
        createdAt: new Date(submissionRow.created_at).toISOString(),
      };

      const outboxSql = `
        INSERT INTO outbox (topic, msg_key, payload)
        VALUES ($1, $2, $3);
      `;
      await client.query(outboxSql, [
        TOPICS.SUBMISSIONS_QUEUED,
        submissionRow.id,
        JSON.stringify(queuedEvent),
      ]);
    }

    // If existing, fetch its test results
    let results: TestResultRow[] = [];
    if (!isCreated) {
      const resultsRes = await client.query<TestResultRow>(
        `SELECT test_index, verdict, time_ms, memory_kb, is_sample
         FROM test_results
         WHERE submission_id = $1
         ORDER BY test_index;`,
        [submissionRow.id]
      );
      results = resultsRes.rows;
    }

    await client.query("COMMIT");

    return {
      submission: formatSubmission(submissionRow, results),
      created: isCreated,
    };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function getSubmissionById(id: string): Promise<Submission | null> {
  const subRes = await query<SubmissionRow>(
    `SELECT id, problem_id, language, handle, code, status, verdict, idempotency_key, created_at, judged_at
     FROM submissions
     WHERE id = $1;`,
    [id]
  );

  const row = subRes.rows[0];
  if (!row) return null;

  const resultsRes = await query<TestResultRow>(
    `SELECT test_index, verdict, time_ms, memory_kb, is_sample
     FROM test_results
     WHERE submission_id = $1
     ORDER BY test_index;`,
    [id]
  );

  return formatSubmission(row, resultsRes.rows);
}

export async function listSubmissionsByHandle(handle: string, limit: number): Promise<Submission[]> {
  const subRes = await query<SubmissionRow>(
    `SELECT id, problem_id, language, handle, code, status, verdict, idempotency_key, created_at, judged_at
     FROM submissions
     WHERE handle = $1
     ORDER BY created_at DESC
     LIMIT $2;`,
    [handle, limit]
  );

  if (subRes.rows.length === 0) return [];

  const submissionIds = subRes.rows.map((r) => r.id);
  const resultsRes = await query<TestResultRow>(
    `SELECT submission_id, test_index, verdict, time_ms, memory_kb, is_sample
     FROM test_results
     WHERE submission_id = ANY($1::uuid[])
     ORDER BY test_index;`,
    [submissionIds]
  );

  const resultsBySubmission = new Map<string, TestResultRow[]>();
  for (const r of resultsRes.rows) {
    const subId = r.submission_id!;
    const list = resultsBySubmission.get(subId) || [];
    list.push(r);
    resultsBySubmission.set(subId, list);
  }

  return subRes.rows.map((r) => formatSubmission(r, resultsBySubmission.get(r.id) || []));
}

export async function getProblemById(id: string): Promise<Problem | null> {
  const probRes = await query<ProblemRow>(
    `SELECT id, title, statement, time_limit_ms, memory_limit_mb
     FROM problems
     WHERE id = $1;`,
    [id]
  );

  const row = probRes.rows[0];
  if (!row) return null;

  const samplesRes = await query<{ input: string; expected_output: string }>(
    `SELECT input, expected_output
     FROM test_cases
     WHERE problem_id = $1 AND is_sample = true
     ORDER BY test_index;`,
    [id]
  );

  return {
    id: row.id,
    title: row.title,
    statement: row.statement,
    timeLimitMs: row.time_limit_ms,
    memoryLimitMb: row.memory_limit_mb,
    samples: samplesRes.rows.map((s) => ({
      input: s.input,
      expectedOutput: s.expected_output,
    })),
  };
}

export async function listProblems(): Promise<Problem[]> {
  const probRes = await query<ProblemRow>(
    `SELECT id, title, statement, time_limit_ms, memory_limit_mb
     FROM problems
     ORDER BY id;`
  );

  if (probRes.rows.length === 0) return [];

  const samplesRes = await query<{ problem_id: string; input: string; expected_output: string }>(
    `SELECT problem_id, input, expected_output
     FROM test_cases
     WHERE is_sample = true
     ORDER BY problem_id, test_index;`
  );

  const samplesByProblem = new Map<string, { input: string; expectedOutput: string }[]>();
  for (const s of samplesRes.rows) {
    const list = samplesByProblem.get(s.problem_id) || [];
    list.push({ input: s.input, expectedOutput: s.expected_output });
    samplesByProblem.set(s.problem_id, list);
  }

  return probRes.rows.map((p) => ({
    id: p.id,
    title: p.title,
    statement: p.statement,
    timeLimitMs: p.time_limit_ms,
    memoryLimitMb: p.memory_limit_mb,
    samples: samplesByProblem.get(p.id) || [],
  }));
}

export async function getJudgingJobData(submissionId: string): Promise<{
  submission: SubmissionRow;
  problem: ProblemRow;
  testCases: JudgeTestCase[];
} | null> {
  const subRes = await query<SubmissionRow>(
    `SELECT id, problem_id, language, handle, code, status, verdict, idempotency_key, created_at, judged_at
     FROM submissions
     WHERE id = $1;`,
    [submissionId]
  );

  const sub = subRes.rows[0];
  if (!sub) return null;

  const probRes = await query<ProblemRow>(
    `SELECT id, title, statement, time_limit_ms, memory_limit_mb
     FROM problems
     WHERE id = $1;`,
    [sub.problem_id]
  );

  const prob = probRes.rows[0];
  if (!prob) throw new Error(`Referenced problem not found: ${sub.problem_id}`);

  const tcRes = await query<TestCaseRow>(
    `SELECT test_index, input, expected_output, is_sample
     FROM test_cases
     WHERE problem_id = $1
     ORDER BY test_index;`,
    [sub.problem_id]
  );

  return {
    submission: sub,
    problem: prob,
    testCases: tcRes.rows.map((tc) => ({
      testIndex: tc.test_index,
      input: tc.input,
      expectedOutput: tc.expected_output,
      isSample: tc.is_sample,
    })),
  };
}
