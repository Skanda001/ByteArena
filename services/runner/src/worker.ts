import Docker from "dockerode";
import { Consumer, Producer, EachMessagePayload } from "kafkajs";
import {
  logger,
  config,
  createKafkaClient,
  createConsumer,
  createProducer,
  publishJsonMessage,
  createJudgeClient,
  JudgeServiceClient,
  TOPICS,
  SubmissionQueuedEventSchema,
  SubmissionQueuedEvent,
  JudgingStartedEvent,
  TestResultEvent,
  FinalVerdictEvent,
  SubmissionDlqEvent,
  VerdictType,
  fromProtoLanguage,
} from "@bytearena/shared";
import type { GetJudgingJobResponse } from "@bytearena/shared/generated/bytearena/v1/GetJudgingJobResponse";
import { runInSandbox, cleanupOrphanContainers } from "./sandbox";

export interface RunnerWorkerOptions {
  clientId?: string;
  groupId?: string;
  grpcAddr?: string;
  sessionTimeoutMs?: number;
  docker?: Docker;
}

export class RunnerWorker {
  private readonly docker: Docker;
  private readonly groupId: string;
  private readonly clientId: string;
  private readonly grpcAddr: string;
  private readonly sessionTimeoutMs: number;
  private consumer: Consumer | null = null;
  private producer: Producer | null = null;
  private judgeClient: JudgeServiceClient | null = null;
  private stopping = false;

  constructor(options: RunnerWorkerOptions = {}) {
    this.docker = options.docker ?? new Docker();
    this.clientId = options.clientId ?? `runner-${process.pid}`;
    this.groupId = options.groupId ?? "runner-group";
    this.grpcAddr = options.grpcAddr ?? config.SUBMISSION_GRPC_ADDR;
    this.sessionTimeoutMs =
      options.sessionTimeoutMs ?? config.RUNNER_SESSION_TIMEOUT_MS;
  }

  async start(): Promise<void> {
    logger.info({ clientId: this.clientId, groupId: this.groupId }, "Starting runner worker...");

    // Clean up any orphan sandbox containers on startup
    const cleaned = await cleanupOrphanContainers(this.docker);
    if (cleaned > 0) {
      logger.info({ cleaned }, "Cleaned up orphan sandbox containers on startup");
    }

    // Connect gRPC JudgeService client
    this.judgeClient = createJudgeClient(this.grpcAddr);

    // Initialize Kafka client, producer, and consumer
    const kafka = createKafkaClient(this.clientId);
    this.producer = await createProducer(kafka, {
      idempotent: true,
      maxInFlightRequests: 1,
    });

    this.consumer = await createConsumer(kafka, {
      groupId: this.groupId,
      sessionTimeout: this.sessionTimeoutMs,
    });

    await this.consumer.subscribe({
      topic: TOPICS.SUBMISSIONS_QUEUED,
      fromBeginning: false,
    });

    logger.info(
      { topic: TOPICS.SUBMISSIONS_QUEUED, groupId: this.groupId },
      "Subscribed to topic, beginning consumption"
    );

    await this.consumer.run({
      autoCommit: false,
      eachMessage: this.processMessage.bind(this),
    });
  }

  async stop(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    logger.info("Stopping runner worker...");

    if (this.consumer) {
      try {
        await this.consumer.disconnect();
        logger.info("Runner Kafka consumer disconnected");
      } catch (err: unknown) {
        logger.warn({ err }, "Error disconnecting runner consumer");
      }
    }

    if (this.producer) {
      try {
        await this.producer.disconnect();
        logger.info("Runner Kafka producer disconnected");
      } catch (err: unknown) {
        logger.warn({ err }, "Error disconnecting runner producer");
      }
    }

    logger.info("Runner worker stopped gracefully");
  }

  private async processMessage({
    topic,
    partition,
    message,
    heartbeat,
  }: EachMessagePayload): Promise<void> {
    const rawValue = message.value?.toString("utf8");
    if (!rawValue) {
      logger.warn({ partition, offset: message.offset }, "Received empty message in queued topic");
      await this.commitOffset(topic, partition, message.offset);
      return;
    }

    let parsedEvent: SubmissionQueuedEvent;
    try {
      const json = JSON.parse(rawValue);
      const parseResult = SubmissionQueuedEventSchema.safeParse(json);
      if (!parseResult.success) {
        logger.error(
          { errors: parseResult.error.format(), offset: message.offset },
          "Invalid SubmissionQueuedEvent format"
        );
        await this.commitOffset(topic, partition, message.offset);
        return;
      }
      parsedEvent = parseResult.data;
    } catch (err: unknown) {
      logger.error({ err, offset: message.offset }, "Failed to parse JSON queued event");
      await this.commitOffset(topic, partition, message.offset);
      return;
    }

    const { submissionId } = parsedEvent;
    const subLogger = logger.child({ submissionId });
    subLogger.info({ partition, offset: message.offset }, "Received queued submission for judging");

    // Retry loop for infrastructure failures (Docker unavailable, gRPC unreachable)
    const MAX_ATTEMPTS = 3;
    let attempts = 0;
    let success = false;
    let lastError: Error | null = null;

    while (attempts < MAX_ATTEMPTS && !this.stopping) {
      attempts++;
      try {
        await this.judgeSubmission(parsedEvent, heartbeat, subLogger);
        success = true;
        break;
      } catch (err: unknown) {
        lastError = err instanceof Error ? err : new Error(String(err));
        subLogger.warn(
          { attempt: attempts, maxAttempts: MAX_ATTEMPTS, err: lastError.message },
          "Infrastructure failure during judging, backing off"
        );

        if (attempts < MAX_ATTEMPTS && !this.stopping) {
          await heartbeat();
          // Exponential backoff: 1s, 2s
          await this.sleep(1000 * Math.pow(2, attempts - 1));
          await heartbeat();
        }
      }
    }

    if (!success && !this.stopping) {
      subLogger.error(
        { attempts, err: lastError?.message },
        "Exceeded max retries for infrastructure failure; routing to DLQ"
      );
      await this.handleInfrastructureFailure(
        submissionId,
        lastError?.message || "Infrastructure failure",
        attempts
      );
    }

    // Always commit the offset once processing (or DLQ routing) is complete
    await this.commitOffset(topic, partition, message.offset);
    subLogger.info({ offset: message.offset }, "Committed offset for submission");
  }

  private async judgeSubmission(
    event: SubmissionQueuedEvent,
    heartbeat: () => Promise<void>,
    subLogger: typeof logger
  ): Promise<void> {
    const { submissionId } = event;

    // 1. Fetch judging job over gRPC
    const job = await this.getJudgingJob(submissionId);

    // 2. Check if already final
    if (job.alreadyFinal) {
      subLogger.info("Submission is already marked final; skipping judging");
      return;
    }

    if (!job.testCases || job.testCases.length === 0) {
      subLogger.warn("Judging job has 0 test cases; publishing empty ACCEPTED");
      await this.publishFinalVerdict(submissionId, "ACCEPTED");
      return;
    }

    // 3. Publish JUDGING_STARTED event
    const judgingStartedEvent: JudgingStartedEvent = {
      type: "JUDGING_STARTED",
      submissionId,
      totalTests: job.testCases.length,
      ts: new Date().toISOString(),
    };

    await publishJsonMessage({
      producer: this.producer!,
      topic: TOPICS.SUBMISSIONS_RESULTS,
      key: submissionId,
      value: judgingStartedEvent,
    });
    subLogger.info({ totalTests: job.testCases.length }, "Published JUDGING_STARTED");

    if (!job.code) {
      throw new Error(`Submission ${submissionId} has empty or missing code`);
    }

    // 4. Run test cases sequentially in test_index order
    const sortedTestCases = [...job.testCases].sort(
      (a, b) => (a.testIndex ?? 0) - (b.testIndex ?? 0)
    );

    let finalVerdict: VerdictType = "ACCEPTED";
    const language = fromProtoLanguage(String(job.language ?? ""));

    for (const testCase of sortedTestCases) {
      if (this.stopping) {
        throw new Error("Worker stopping during judging");
      }

      await heartbeat();

      const sandboxResult = await runInSandbox({
        submissionId,
        language,
        code: job.code,
        stdin: testCase.input ?? "",
        expectedOutput: testCase.expectedOutput ?? "",
        timeLimitMs: job.timeLimitMs ?? undefined,
        memoryLimitMb: job.memoryLimitMb ?? undefined,
        startupAllowanceMs: config.SANDBOX_STARTUP_ALLOWANCE_MS,
        docker: this.docker,
      });

      // The final verdict is the first non-accepted verdict in test order, otherwise ACCEPTED
      if (finalVerdict === "ACCEPTED" && sandboxResult.outcome !== "ACCEPTED") {
        finalVerdict = sandboxResult.outcome;
      }

      // 5. Publish TEST_RESULT event (never include test input or expected output)
      const testResultEvent: TestResultEvent = {
        type: "TEST_RESULT",
        submissionId,
        testIndex: testCase.testIndex ?? 1,
        verdict: sandboxResult.outcome,
        timeMs: sandboxResult.timeMs,
        memoryKb: sandboxResult.memoryKb,
        isSample: testCase.isSample ?? false,
        ts: new Date().toISOString(),
      };

      await publishJsonMessage({
        producer: this.producer!,
        topic: TOPICS.SUBMISSIONS_RESULTS,
        key: submissionId,
        value: testResultEvent,
      });

      subLogger.info(
        {
          testIndex: testCase.testIndex,
          verdict: sandboxResult.outcome,
          timeMs: sandboxResult.timeMs,
          isSample: testCase.isSample,
        },
        "Published TEST_RESULT"
      );

      await heartbeat();
    }

    // 6. Publish FINAL_VERDICT event
    await this.publishFinalVerdict(submissionId, finalVerdict);
    subLogger.info({ finalVerdict }, "Published FINAL_VERDICT");
  }

  private async publishFinalVerdict(
    submissionId: string,
    verdict: VerdictType
  ): Promise<void> {
    const finalVerdictEvent: FinalVerdictEvent = {
      type: "FINAL_VERDICT",
      submissionId,
      verdict,
      ts: new Date().toISOString(),
    };

    await publishJsonMessage({
      producer: this.producer!,
      topic: TOPICS.SUBMISSIONS_RESULTS,
      key: submissionId,
      value: finalVerdictEvent,
    });
  }

  private async handleInfrastructureFailure(
    submissionId: string,
    reason: string,
    attempts: number
  ): Promise<void> {
    // Publish to submissions.dlq
    const dlqEvent: SubmissionDlqEvent = {
      submissionId,
      reason,
      attempts,
      ts: new Date().toISOString(),
    };

    await publishJsonMessage({
      producer: this.producer!,
      topic: TOPICS.SUBMISSIONS_DLQ,
      key: submissionId,
      value: dlqEvent,
    });

    // Publish FINAL_VERDICT with INTERNAL_ERROR
    await this.publishFinalVerdict(submissionId, "INTERNAL_ERROR");
  }

  private async commitOffset(
    topic: string,
    partition: number,
    offset: string
  ): Promise<void> {
    if (!this.consumer) return;
    const nextOffset = (BigInt(offset) + 1n).toString();
    await this.consumer.commitOffsets([
      {
        topic,
        partition,
        offset: nextOffset,
      },
    ]);
  }

  private getJudgingJob(submissionId: string): Promise<GetJudgingJobResponse> {
    return new Promise((resolve, reject) => {
      if (!this.judgeClient) {
        return reject(new Error("JudgeServiceClient is not initialized"));
      }

      this.judgeClient.getJudgingJob({ submissionId }, (err, response) => {
        if (err) {
          return reject(err);
        }
        if (!response) {
          return reject(
            new Error("Empty response received from JudgeService.GetJudgingJob")
          );
        }
        resolve(response);
      });
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
