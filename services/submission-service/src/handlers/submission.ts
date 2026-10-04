import * as grpc from "@grpc/grpc-js";
import {
  fromProtoLanguage,
  logger,
  LanguageType,
} from "@bytearena/shared";
import type { SubmissionServiceHandlers } from "@bytearena/shared";
import {
  createSubmissionTx,
  getProblemById,
  getSubmissionById,
  listProblems,
  listSubmissionsByHandle,
} from "../db";

const HANDLE_REGEX = /^[a-zA-Z0-9_-]{1,32}$/;

export const submissionHandlers: SubmissionServiceHandlers = {
  CreateSubmission: async (call, callback) => {
    try {
      const { problemId, language, code, handle, idempotencyKey } = call.request;

      // 1. Validate handle
      if (!handle || !HANDLE_REGEX.test(handle)) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "Handle must be 1 to 32 alphanumeric characters, underscores, or hyphens",
        });
      }

      // 2. Validate language
      let validatedLanguage: LanguageType;
      try {
        validatedLanguage = fromProtoLanguage(language);
      } catch {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Invalid or unsupported language: ${language}`,
        });
      }

      // 3. Validate code length (characters and byte length)
      if (!code || code.length < 1 || code.length > 65536) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "Code character length must be between 1 and 65,536",
        });
      }

      const byteLength = Buffer.byteLength(code, "utf8");
      if (byteLength > 65536) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: `Code byte length exceeds 65,536 bytes (actual: ${byteLength} bytes)`,
        });
      }

      // 4. Validate problem existence
      if (!problemId) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "problemId is required",
        });
      }

      const problem = await getProblemById(problemId);
      if (!problem) {
        return callback({
          code: grpc.status.NOT_FOUND,
          message: `Problem not found: ${problemId}`,
        });
      }

      // 5. Clean idempotency key
      const cleanIdempotencyKey =
        idempotencyKey && idempotencyKey.trim().length > 0 ? idempotencyKey.trim() : null;

      // 6. Transactional insert with outbox
      const result = await createSubmissionTx({
        problemId: problemId,
        language: validatedLanguage,
        handle,
        code,
        idempotencyKey: cleanIdempotencyKey,
      });

      logger.info(
        {
          submissionId: result.submission.id,
          handle,
          problemId,
          created: result.created,
        },
        "Processed CreateSubmission request"
      );

      return callback(null, {
        submission: result.submission,
        created: result.created,
      });
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger.error({ err: error }, "Failed to execute CreateSubmission");
      return callback({
        code: grpc.status.INTERNAL,
        message: error.message,
      });
    }
  },

  GetSubmission: async (call, callback) => {
    try {
      const { id } = call.request;
      if (!id) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "id is required",
        });
      }

      const submission = await getSubmissionById(id);
      if (!submission) {
        return callback({
          code: grpc.status.NOT_FOUND,
          message: `Submission not found: ${id}`,
        });
      }

      return callback(null, submission);
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger.error({ err: error, id: call.request.id }, "Failed to execute GetSubmission");
      return callback({
        code: grpc.status.INTERNAL,
        message: error.message,
      });
    }
  },

  ListSubmissions: async (call, callback) => {
    try {
      const { handle, limit } = call.request;
      if (!handle || !HANDLE_REGEX.test(handle)) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "Valid handle is required",
        });
      }

      const safeLimit = limit && limit > 0 ? Math.min(limit, 100) : 20;
      const submissions = await listSubmissionsByHandle(handle, safeLimit);

      return callback(null, { submissions });
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger.error({ err: error, handle: call.request.handle }, "Failed to execute ListSubmissions");
      return callback({
        code: grpc.status.INTERNAL,
        message: error.message,
      });
    }
  },

  GetProblem: async (call, callback) => {
    try {
      const { id } = call.request;
      if (!id) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: "id is required",
        });
      }

      const problem = await getProblemById(id);
      if (!problem) {
        return callback({
          code: grpc.status.NOT_FOUND,
          message: `Problem not found: ${id}`,
        });
      }

      return callback(null, problem);
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger.error({ err: error, id: call.request.id }, "Failed to execute GetProblem");
      return callback({
        code: grpc.status.INTERNAL,
        message: error.message,
      });
    }
  },

  ListProblems: async (_call, callback) => {
    try {
      const problems = await listProblems();
      return callback(null, { problems });
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger.error({ err: error }, "Failed to execute ListProblems");
      return callback({
        code: grpc.status.INTERNAL,
        message: error.message,
      });
    }
  },
};
