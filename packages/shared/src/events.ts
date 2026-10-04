import { z } from "zod";

export const TOPICS = {
  SUBMISSIONS_QUEUED: "submissions.queued",
  SUBMISSIONS_RESULTS: "submissions.results",
  SUBMISSIONS_DLQ: "submissions.dlq",
} as const;

export const LanguageSchema = z.enum(["PYTHON", "JAVASCRIPT"]);
export type LanguageType = z.infer<typeof LanguageSchema>;

export const SubmissionStatusSchema = z.enum([
  "QUEUED",
  "JUDGING",
  "COMPLETED",
  "SYSTEM_ERROR",
]);
export type SubmissionStatusType = z.infer<typeof SubmissionStatusSchema>;

export const VerdictSchema = z.enum([
  "ACCEPTED",
  "WRONG_ANSWER",
  "RUNTIME_ERROR",
  "TIME_LIMIT_EXCEEDED",
  "MEMORY_LIMIT_EXCEEDED",
  "OUTPUT_LIMIT_EXCEEDED",
  "INTERNAL_ERROR",
]);
export type VerdictType = z.infer<typeof VerdictSchema>;

export const SubmissionQueuedEventSchema = z.object({
  submissionId: z.string().uuid(),
  problemId: z.string().min(1),
  language: LanguageSchema,
  createdAt: z.string().datetime({ offset: true }),
});
export type SubmissionQueuedEvent = z.infer<typeof SubmissionQueuedEventSchema>;

export const JudgingStartedEventSchema = z.object({
  type: z.literal("JUDGING_STARTED"),
  submissionId: z.string().uuid(),
  totalTests: z.number().int().positive(),
  ts: z.string().datetime({ offset: true }),
});
export type JudgingStartedEvent = z.infer<typeof JudgingStartedEventSchema>;

export const TestResultEventSchema = z.object({
  type: z.literal("TEST_RESULT"),
  submissionId: z.string().uuid(),
  testIndex: z.number().int().positive(),
  verdict: VerdictSchema,
  timeMs: z.number().int().nonnegative(),
  memoryKb: z.number().int().nonnegative(),
  isSample: z.boolean(),
  ts: z.string().datetime({ offset: true }),
});
export type TestResultEvent = z.infer<typeof TestResultEventSchema>;

export const FinalVerdictEventSchema = z.object({
  type: z.literal("FINAL_VERDICT"),
  submissionId: z.string().uuid(),
  verdict: VerdictSchema,
  ts: z.string().datetime({ offset: true }),
});
export type FinalVerdictEvent = z.infer<typeof FinalVerdictEventSchema>;

export const SubmissionResultEventSchema = z.discriminatedUnion("type", [
  JudgingStartedEventSchema,
  TestResultEventSchema,
  FinalVerdictEventSchema,
]);
export type SubmissionResultEvent = z.infer<typeof SubmissionResultEventSchema>;

export const SubmissionDlqEventSchema = z.object({
  submissionId: z.string().uuid(),
  reason: z.string().min(1),
  attempts: z.number().int().positive(),
  ts: z.string().datetime({ offset: true }),
});
export type SubmissionDlqEvent = z.infer<typeof SubmissionDlqEventSchema>;
