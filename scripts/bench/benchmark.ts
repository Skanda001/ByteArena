import os from "os";
import {
  pool,
  closeDb,
  createSubmissionClient,
  toProtoLanguage,
} from "@bytearena/shared";
import type { CreateSubmissionResponse } from "@bytearena/shared/generated/bytearena/v1/CreateSubmissionResponse";

interface BenchmarkOptions {
  total: number;
  concurrency: number;
  problemId: string;
  language: "PYTHON" | "JAVASCRIPT";
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseArgs(): BenchmarkOptions {
  const args = process.argv.slice(2);
  let total = 50;
  let concurrency = 5;
  let problemId = "sum-two";
  let language: "PYTHON" | "JAVASCRIPT" = "PYTHON";

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--total" && args[i + 1]) {
      total = parseInt(args[i + 1], 10);
      i++;
    } else if (args[i] === "--concurrency" && args[i + 1]) {
      concurrency = parseInt(args[i + 1], 10);
      i++;
    } else if (args[i] === "--problem" && args[i + 1]) {
      problemId = args[i + 1];
      i++;
    } else if (args[i] === "--language" && args[i + 1]) {
      language = args[i + 1].toUpperCase() as "PYTHON" | "JAVASCRIPT";
      i++;
    }
  }

  return { total, concurrency, problemId, language };
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const index = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  const weight = index - lower;
  if (lower === upper) return sorted[index];
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

async function main(): Promise<void> {
  const opts = parseArgs();
  console.log("===============================================================");
  console.log("             ByteArena Benchmark Load Generator                ");
  console.log("===============================================================");
  console.log(`Submissions Total:   ${opts.total}`);
  console.log(`Concurrency Level:   ${opts.concurrency}`);
  console.log(`Problem ID:          ${opts.problemId}`);
  console.log(`Language:            ${opts.language}`);
  console.log("---------------------------------------------------------------");

  const grpcAddr = process.env.SUBMISSION_GRPC_ADDR || "localhost:50051";
  const submissionClient = createSubmissionClient(grpcAddr);

  const pythonCode = "a, b = map(int, input().split())\nprint(a + b)";
  const jsCode = `const fs = require('fs');
const s = fs.readFileSync(0, 'utf-8').trim();
if (!s) process.exit(0);
const [a, b] = s.split(/\\s+/).map(Number);
console.log(a + b);`;

  const code = opts.language === "PYTHON" ? pythonCode : jsCode;

  // Machine specs
  const cpus = os.cpus();
  const cpuModel = cpus[0]?.model ?? "Unknown CPU";
  const cpuCores = cpus.length;
  const totalRamGb = (os.totalmem() / 1024 / 1024 / 1024).toFixed(1);
  const osInfo = `${os.type()} ${os.release()} (${os.arch()})`;

  console.log(`Machine OS:          ${osInfo}`);
  console.log(`Machine CPU:         ${cpuModel} (${cpuCores} vCPUs)`);
  console.log(`Machine RAM:         ${totalRamGb} GB`);
  console.log("===============================================================\n");

  const latenciesMs: number[] = [];
  let completedCount = 0;
  let failedCount = 0;

  let nextIndex = 0;
  const startTime = Date.now();

  async function worker(workerId: number): Promise<void> {
    while (true) {
      const idx = nextIndex++;
      if (idx >= opts.total) break;

      const tSubmit = Date.now();
      const idempotencyKey = `bench-${Date.now()}-${workerId}-${idx}-${Math.random().toString(36).slice(2, 7)}`;

      try {
        const createRes = await new Promise<CreateSubmissionResponse>(
          (resolve, reject) => {
            submissionClient.createSubmission(
              {
                problemId: opts.problemId,
                language: toProtoLanguage(opts.language),
                code,
                handle: `bench-user-${workerId}`,
                idempotencyKey,
              },
              (err, res) => {
                if (err) return reject(err);
                resolve(res!);
              }
            );
          }
        );

        const subId = createRes.submission?.id;
        if (!subId) {
          throw new Error("Missing submission ID in response");
        }

        // Poll PostgreSQL until COMPLETED or timeout
        let isComplete = false;
        const deadline = Date.now() + 60000;

        while (Date.now() < deadline) {
          const res = await pool.query(
            "SELECT status, verdict FROM submissions WHERE id = $1",
            [subId]
          );
          const row = res.rows[0];
          if (row?.status === "COMPLETED") {
            isComplete = true;
            break;
          }
          if (row?.status === "SYSTEM_ERROR") {
            throw new Error(`Submission ${subId} failed with SYSTEM_ERROR`);
          }
          await sleep(100);
        }

        if (!isComplete) {
          throw new Error(`Submission ${subId} timed out`);
        }

        const tComplete = Date.now();
        const latency = tComplete - tSubmit;
        latenciesMs.push(latency);
        completedCount++;

        process.stdout.write(
          `\rProgress: [${completedCount}/${opts.total}] completed | Current latency: ${latency}ms   `
        );
      } catch (err) {
        failedCount++;
        console.error(`\nError processing submission #${idx}:`, err);
      }
    }
  }

  // Launch concurrency workers
  const workers: Promise<void>[] = [];
  for (let i = 0; i < opts.concurrency; i++) {
    workers.push(worker(i + 1));
  }

  await Promise.all(workers);
  const totalDurationSec = (Date.now() - startTime) / 1000;
  console.log("\n\nAll benchmark tasks completed.\n");

  // Statistical calculations
  latenciesMs.sort((a, b) => a - b);
  const min = latenciesMs[0] ?? 0;
  const max = latenciesMs[latenciesMs.length - 1] ?? 0;
  const sum = latenciesMs.reduce((acc, v) => acc + v, 0);
  const mean = latenciesMs.length > 0 ? Math.round(sum / latenciesMs.length) : 0;
  const p50 = Math.round(percentile(latenciesMs, 50));
  const p90 = Math.round(percentile(latenciesMs, 90));
  const p95 = Math.round(percentile(latenciesMs, 95));
  const p99 = Math.round(percentile(latenciesMs, 99));
  const throughput = (completedCount / totalDurationSec).toFixed(2);

  console.log("===============================================================");
  console.log("                   Benchmark Results Summary                  ");
  console.log("===============================================================");
  console.log(`Total Completed:     ${completedCount} / ${opts.total}`);
  console.log(`Total Failed:        ${failedCount}`);
  console.log(`Total Time:          ${totalDurationSec.toFixed(2)}s`);
  console.log(`Throughput:          ${throughput} submissions/sec`);
  console.log("---------------------------------------------------------------");
  console.log(`Latency Min:         ${min}ms`);
  console.log(`Latency Mean:        ${mean}ms`);
  console.log(`Latency p50:         ${p50}ms`);
  console.log(`Latency p90:         ${p90}ms`);
  console.log(`Latency p95:         ${p95}ms`);
  console.log(`Latency p99:         ${p99}ms`);
  console.log(`Latency Max:         ${max}ms`);
  console.log("===============================================================\n");

  console.log("### Markdown Table for Documentation:\n");
  console.log(
    `| Metric | Value | How measured | Machine |`
  );
  console.log(`|---|---|---|---|`);
  console.log(
    `| Judge latency p50 (sum-two, Python) | ${p50}ms | \`scripts/bench/benchmark.ts\` (50 submissions, concurrency 5, 5 tests each) | ${cpuModel} (${cpuCores} vCPUs), ${totalRamGb}GB RAM, ${osInfo} |`
  );
  console.log(
    `| Judge latency p95 | ${p95}ms | \`scripts/bench/benchmark.ts\` (50 submissions, concurrency 5, 5 tests each) | ${cpuModel} (${cpuCores} vCPUs), ${totalRamGb}GB RAM, ${osInfo} |`
  );
  console.log(
    `| Throughput | ${throughput} sub/s | \`scripts/bench/benchmark.ts\` (50 submissions, concurrency 5, 5 tests each) | ${cpuModel} (${cpuCores} vCPUs), ${totalRamGb}GB RAM, ${osInfo} |`
  );

  await closeDb();
  process.exit(failedCount === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error("Fatal benchmark error:", err);
  process.exit(1);
});
