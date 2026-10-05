import { execSync } from "child_process";
import * as path from "path";

interface ChaosResult {
  name: string;
  script: string;
  status: "PASS" | "FAIL";
  durationMs: number;
  output: string;
}

const TESTS = [
  { name: "Runner Mid-Judging Crash & Recovery", script: "kill-runner.ts" },
  { name: "Outbox Publisher Crash & Drain", script: "kill-publisher.ts" },
  { name: "Kafka Broker Outage Tolerance", script: "kafka-down.ts" },
  { name: "Duplicate Delivery & Idempotency", script: "duplicate-delivery.ts" },
  { name: "Infrastructure Failure & DLQ Routing", script: "dlq-failure.ts" },
];

async function main(): Promise<void> {
  console.log("===============================================================");
  console.log("             ByteArena Phase 7: Chaos Test Suite               ");
  console.log("===============================================================\n");

  const results: ChaosResult[] = [];
  let allPassed = true;

  for (const test of TESTS) {
    console.log(`\n>>> Running: ${test.name} (${test.script})`);
    const scriptPath = path.join(__dirname, test.script);
    const start = Date.now();

    try {
      const output = execSync(`npx tsx "${scriptPath}"`, {
        encoding: "utf-8",
        stdio: "pipe",
        timeout: 120000,
      });
      const durationMs = Date.now() - start;
      console.log(output);
      results.push({
        name: test.name,
        script: test.script,
        status: "PASS",
        durationMs,
        output,
      });
    } catch (err: unknown) {
      allPassed = false;
      const durationMs = Date.now() - start;
      const errorOutput =
        err && typeof err === "object" && "stdout" in err
          ? String((err as { stdout: unknown }).stdout) +
            "\n" +
            String((err as { stderr: unknown }).stderr)
          : String(err);
      console.error(errorOutput);
      results.push({
        name: test.name,
        script: test.script,
        status: "FAIL",
        durationMs,
        output: errorOutput,
      });
    }
  }

  console.log("\n===============================================================");
  console.log("                   Chaos Test Summary Report                  ");
  console.log("===============================================================");

  for (const r of results) {
    const paddedName = r.name.padEnd(45, " ");
    const duration = `${(r.durationMs / 1000).toFixed(1)}s`.padStart(6, " ");
    const tag = r.status === "PASS" ? " [PASS] " : " [FAIL] ";
    console.log(`${paddedName} ${tag} (${duration})`);
  }
  console.log("===============================================================\n");

  try {
    execSync("docker compose up -d", { stdio: "ignore" });
  } catch {
    // ignore
  }

  if (allPassed) {
    console.log("ALL CHAOS TESTS PASSED!");
    process.exit(0);
  } else {
    console.error("SOME CHAOS TESTS FAILED");
    process.exit(1);
  }
}

main();
