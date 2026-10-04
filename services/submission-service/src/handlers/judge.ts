import * as grpc from "@grpc/grpc-js";
import {
  logger,
  toProtoLanguage,
  LanguageType,
} from "@bytearena/shared";
import type { JudgeServiceHandlers } from "@bytearena/shared";
import { getJudgingJobData } from "../db";

export const judgeHandlers: JudgeServiceHandlers = {
  GetJudgingJob: async (call, callback) => {
    try {
      const { submissionId } = call.request;
      if (!submissionId) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "submissionId is required",
        });
      }

      const jobData = await getJudgingJobData(submissionId);
      if (!jobData) {
        return callback({
          code: grpc.status.NOT_FOUND,
          message: `Submission not found: ${submissionId}`,
        });
      }

      const { submission, problem, testCases } = jobData;
      const alreadyFinal =
        submission.status === "COMPLETED" || submission.status === "SYSTEM_ERROR";

      logger.info(
        {
          submissionId: submission.id,
          status: submission.status,
          alreadyFinal,
          totalTestCases: testCases.length,
        },
        "Fetched GetJudgingJob request"
      );

      return callback(null, {
        alreadyFinal,
        submissionId: submission.id,
        problemId: submission.problem_id,
        language: toProtoLanguage(submission.language as LanguageType),
        code: submission.code,
        timeLimitMs: alreadyFinal ? 0 : problem.time_limit_ms,
        memoryLimitMb: alreadyFinal ? 0 : problem.memory_limit_mb,
        testCases: alreadyFinal ? [] : testCases,
      });
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger.error(
        { err: error, submissionId: call.request.submissionId },
        "Failed to execute GetJudgingJob"
      );
      return callback({
        code: grpc.status.INTERNAL,
        message: error.message,
      });
    }
  },
};
