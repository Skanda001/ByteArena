import { GraphQLError } from "graphql";
import {
  fromProtoLanguage,
  toProtoLanguage,
  fromProtoStatus,
  fromProtoVerdict,
  VerdictType,
  LanguageType,
  SubmissionResultEvent,
  logger,
} from "@bytearena/shared";
import type { SubmissionServiceClient } from "@bytearena/shared";
import type { Problem as ProtoProblem } from "@bytearena/shared/generated/bytearena/v1/Problem";
import type { Submission as ProtoSubmission } from "@bytearena/shared/generated/bytearena/v1/Submission";
import type { CreateSubmissionResponse } from "@bytearena/shared/generated/bytearena/v1/CreateSubmissionResponse";
import type { ListProblemsResponse } from "@bytearena/shared/generated/bytearena/v1/ListProblemsResponse";
import type { ListSubmissionsResponse } from "@bytearena/shared/generated/bytearena/v1/ListSubmissionsResponse";
import { RateLimiter } from "./rate-limiter";
import { KafkaBridge } from "./kafka-bridge";

export interface ResolverContext {
  submissionClient: SubmissionServiceClient;
  rateLimiter: RateLimiter;
  kafkaBridge: KafkaBridge;
}

function safeFromProtoVerdict(protoVerdict?: string | number | null): VerdictType | null {
  if (!protoVerdict || protoVerdict === "VERDICT_UNSPECIFIED" || protoVerdict === 0) return null;
  return fromProtoVerdict(String(protoVerdict));
}

function mapProblem(p: ProtoProblem) {
  return {
    id: p.id,
    title: p.title,
    statement: p.statement,
    timeLimitMs: p.timeLimitMs,
    memoryLimitMb: p.memoryLimitMb,
    samples: (p.samples ?? []).map((s) => ({
      input: s.input,
      expectedOutput: s.expectedOutput,
    })),
  };
}

function mapSubmission(s: ProtoSubmission) {
  return {
    id: s.id,
    problemId: s.problemId,
    language: fromProtoLanguage(String(s.language ?? "LANGUAGE_UNSPECIFIED")),
    handle: s.handle,
    status: fromProtoStatus(String(s.status ?? "STATUS_QUEUED")),
    verdict: safeFromProtoVerdict(s.verdict),
    createdAt: s.createdAt,
    judgedAt: s.judgedAt ? s.judgedAt : null,
    results: (s.results ?? []).map((r) => ({
      testIndex: r.testIndex,
      verdict: fromProtoVerdict(String(r.verdict ?? "VERDICT_UNSPECIFIED")),
      timeMs: r.timeMs ?? 0,
      memoryKb: r.memoryKb ?? 0,
      isSample: r.isSample ?? false,
    })),
  };
}

export const resolvers = {
  Query: {
    problems: async (_: unknown, __: unknown, { submissionClient }: ResolverContext) => {
      return new Promise((resolve, reject) => {
        submissionClient.listProblems({}, (err, res?: ListProblemsResponse) => {
          if (err) return reject(new GraphQLError(err.message));
          resolve((res?.problems ?? []).map(mapProblem));
        });
      });
    },

    problem: async (_: unknown, { id }: { id: string }, { submissionClient }: ResolverContext) => {
      return new Promise((resolve, _reject) => {
        submissionClient.getProblem({ id }, (err, res?: ProtoProblem) => {
          if (err) {
            // NOT_FOUND in gRPC
            return resolve(null);
          }
          resolve(res ? mapProblem(res) : null);
        });
      });
    },

    submission: async (
      _: unknown,
      { id }: { id: string },
      { submissionClient }: ResolverContext
    ) => {
      return new Promise((resolve, _reject) => {
        submissionClient.getSubmission({ id }, (err, res?: ProtoSubmission) => {
          if (err) {
            return resolve(null);
          }
          resolve(res ? mapSubmission(res) : null);
        });
      });
    },

    submissions: async (
      _: unknown,
      { handle, limit }: { handle: string; limit?: number },
      { submissionClient }: ResolverContext
    ) => {
      return new Promise((resolve, reject) => {
        submissionClient.listSubmissions(
          { handle, limit: limit ?? 20 },
          (err, res?: ListSubmissionsResponse) => {
            if (err) return reject(new GraphQLError(err.message));
            resolve((res?.submissions ?? []).map(mapSubmission));
          }
        );
      });
    },
  },

  Mutation: {
    submitSolution: async (
      _: unknown,
      {
        input,
      }: {
        input: {
          problemId: string;
          language: LanguageType;
          code: string;
          handle: string;
          idempotencyKey?: string | null;
        };
      },
      { submissionClient, rateLimiter }: ResolverContext
    ) => {
      // 1. Rate limiting check (5 submissions per 10 seconds per handle)
      const limitCheck = rateLimiter.check(input.handle);
      if (!limitCheck.allowed) {
        const retrySec = Math.ceil((limitCheck.retryAfterMs ?? 1000) / 1000);
        throw new GraphQLError(
          `Rate limit exceeded: maximum 5 submissions per 10 seconds. Try again in ${retrySec}s.`,
          {
            extensions: {
              code: "RATE_LIMITED",
              retryAfterMs: limitCheck.retryAfterMs,
            },
          }
        );
      }

      // 2. Call SubmissionService.CreateSubmission over gRPC
      return new Promise((resolve, reject) => {
        submissionClient.createSubmission(
          {
            problemId: input.problemId,
            language: toProtoLanguage(input.language),
            code: input.code,
            handle: input.handle,
            idempotencyKey: input.idempotencyKey ?? undefined,
          },
          (err, res?: CreateSubmissionResponse) => {
            if (err) {
              return reject(new GraphQLError(err.message));
            }
            if (!res || !res.submission) {
              return reject(new GraphQLError("Missing submission in gRPC response"));
            }
            resolve({
              submission: mapSubmission(res.submission),
              created: res.created ?? false,
            });
          }
        );
      });
    },
  },

  Subscription: {
    submissionProgress: {
      subscribe: async function* (
        _: unknown,
        { submissionId }: { submissionId: string },
        { submissionClient, kafkaBridge }: ResolverContext
      ) {
        logger.info({ submissionId }, "Client connected to submissionProgress subscription");

        // 1. Subscribe to the Kafka bridge FIRST so no events in flight are missed
        const queue: SubmissionResultEvent[] = [];
        let notifyQueue: (() => void) | null = null;
        let isClosed = false;

        const unsubscribe = kafkaBridge.subscribe(submissionId, (event) => {
          queue.push(event);
          if (notifyQueue) {
            notifyQueue();
            notifyQueue = null;
          }
        });

        const seenKeys = new Set<string>();

        try {
          // 2. Query the current snapshot over gRPC to replay finished state
          const snapshot = await new Promise<ProtoSubmission | null>((resolve) => {
            submissionClient.getSubmission({ id: submissionId }, (err, res?: ProtoSubmission) => {
              if (err) return resolve(null);
              resolve(res ?? null);
            });
          });

          if (snapshot) {
            const hasStarted =
              snapshot.status !== "STATUS_QUEUED" || (snapshot.results && snapshot.results.length > 0);

            if (hasStarted) {
              seenKeys.add("JUDGING_STARTED");
              yield {
                submissionProgress: {
                  submissionId,
                  type: "JUDGING_STARTED",
                  totalTests: 5,
                  testIndex: null,
                  verdict: null,
                  timeMs: null,
                  memoryKb: null,
                  isSample: null,
                },
              };
            }

            if (snapshot.results && snapshot.results.length > 0) {
              for (const r of snapshot.results) {
                const key = `TEST_RESULT:${r.testIndex}`;
                seenKeys.add(key);
                yield {
                  submissionProgress: {
                    submissionId,
                    type: "TEST_RESULT",
                    totalTests: null,
                    testIndex: r.testIndex,
                    verdict: fromProtoVerdict(String(r.verdict ?? "VERDICT_UNSPECIFIED")),
                    timeMs: r.timeMs ?? 0,
                    memoryKb: r.memoryKb ?? 0,
                    isSample: r.isSample ?? false,
                  },
                };
              }
            }

            const isFinal =
              snapshot.status === "STATUS_COMPLETED" ||
              snapshot.status === "STATUS_SYSTEM_ERROR";

            if (isFinal) {
              const finalVerdict = safeFromProtoVerdict(snapshot.verdict);
              seenKeys.add("FINAL_VERDICT");
              yield {
                submissionProgress: {
                  submissionId,
                  type: "FINAL_VERDICT",
                  totalTests: null,
                  testIndex: null,
                  verdict: finalVerdict,
                  timeMs: null,
                  memoryKb: null,
                  isSample: null,
                },
              };
              // Already completed — complete subscription immediately!
              return;
            }
          }

          // 3. Process live stream from queue until FINAL_VERDICT
          while (!isClosed) {
            if (queue.length === 0) {
              await new Promise<void>((resolve) => {
                notifyQueue = resolve;
              });
            }

            while (queue.length > 0) {
              const ev = queue.shift()!;
              let dedupeKey = "";

              if (ev.type === "JUDGING_STARTED") {
                dedupeKey = "JUDGING_STARTED";
              } else if (ev.type === "TEST_RESULT") {
                dedupeKey = `TEST_RESULT:${ev.testIndex}`;
              } else if (ev.type === "FINAL_VERDICT") {
                dedupeKey = "FINAL_VERDICT";
              }

              if (!seenKeys.has(dedupeKey)) {
                seenKeys.add(dedupeKey);

                if (ev.type === "JUDGING_STARTED") {
                  yield {
                    submissionProgress: {
                      submissionId,
                      type: "JUDGING_STARTED",
                      totalTests: ev.totalTests,
                      testIndex: null,
                      verdict: null,
                      timeMs: null,
                      memoryKb: null,
                      isSample: null,
                    },
                  };
                } else if (ev.type === "TEST_RESULT") {
                  yield {
                    submissionProgress: {
                      submissionId,
                      type: "TEST_RESULT",
                      totalTests: null,
                      testIndex: ev.testIndex,
                      verdict: ev.verdict,
                      timeMs: ev.timeMs,
                      memoryKb: ev.memoryKb,
                      isSample: ev.isSample,
                    },
                  };
                } else if (ev.type === "FINAL_VERDICT") {
                  yield {
                    submissionProgress: {
                      submissionId,
                      type: "FINAL_VERDICT",
                      totalTests: null,
                      testIndex: null,
                      verdict: ev.verdict,
                      timeMs: null,
                      memoryKb: null,
                      isSample: null,
                    },
                  };
                  // Subscription finishes on FINAL_VERDICT per contract
                  return;
                }
              }
            }
          }
        } finally {
          isClosed = true;
          unsubscribe();
          logger.info({ submissionId }, "Closed submissionProgress subscription");
        }
      },
    },
  },
};
