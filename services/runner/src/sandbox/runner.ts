import Docker from "dockerode";
import { PassThrough } from "stream";
import { RunInSandboxOptions, SandboxResult, SandboxOutcome } from "./types";
import { compareOutput } from "./comparer";

export const MAX_CODE_BYTES = 64 * 1024; // 64 KiB
export const MAX_OUTPUT_BYTES = 64 * 1024; // 64 KiB
export const MAX_STDERR_CHARS = 4096;
export const DEFAULT_TIME_LIMIT_MS = 2000;
export const DEFAULT_MEMORY_LIMIT_MB = 128;
export const DEFAULT_STARTUP_ALLOWANCE_MS = 500;
export const SANDBOX_CPU_NANOCPUS = 500_000_000; // 0.5 CPU
export const SANDBOX_PIDS_LIMIT = 64;

const SANDBOX_CONFIG = {
  PYTHON: {
    image: "python:3.12-alpine",
    cmd: [
      "python3",
      "-B",
      "-c",
      "import os;c=os.environ.pop('SUBMISSION_CODE');exec(compile(c,'main.py','exec'),{'__name__':'__main__'})",
    ],
  },
  JAVASCRIPT: {
    image: "node:20-alpine",
    cmd: [
      "node",
      "-e",
      "const c=process.env.SUBMISSION_CODE;delete process.env.SUBMISSION_CODE;eval(c)",
    ],
  },
} as const;

export async function runInSandbox(
  options: RunInSandboxOptions
): Promise<SandboxResult> {
  const codeByteLength = Buffer.byteLength(options.code, "utf8");
  if (codeByteLength > MAX_CODE_BYTES) {
    throw new Error(
      `Submission code exceeds maximum allowed size of 64 KiB (${codeByteLength} bytes)`
    );
  }

  const langConfig = SANDBOX_CONFIG[options.language];
  if (!langConfig) {
    throw new Error(`Unsupported sandbox language: ${options.language}`);
  }

  const docker = options.docker ?? new Docker();
  const timeLimitMs = options.timeLimitMs ?? DEFAULT_TIME_LIMIT_MS;
  const memoryLimitMb = options.memoryLimitMb ?? DEFAULT_MEMORY_LIMIT_MB;
  const startupAllowanceMs =
    options.startupAllowanceMs ?? DEFAULT_STARTUP_ALLOWANCE_MS;
  const totalTimeoutMs = timeLimitMs + startupAllowanceMs;
  const memoryLimitBytes = memoryLimitMb * 1024 * 1024;

  const submissionLabel = options.submissionId ?? "sandbox";

  let container: Docker.Container | null = null;
  let timer: NodeJS.Timeout | null = null;
  let timedOut = false;
  let outputCapExceeded = false;
  let logStream: NodeJS.ReadableStream | null = null;

  try {
    container = await docker.createContainer({
      Image: langConfig.image,
      Cmd: langConfig.cmd as unknown as string[],
      Env: [`SUBMISSION_CODE=${options.code}`],
      AttachStdin: true,
      AttachStdout: true,
      AttachStderr: true,
      OpenStdin: true,
      StdinOnce: true,
      Tty: false,
      WorkingDir: "/tmp",
      User: "65534:65534",
      Labels: {
        "bytearena.submission": submissionLabel,
        "bytearena.created_at": Date.now().toString(),
      },
      HostConfig: {
        NetworkMode: "none",
        Memory: memoryLimitBytes,
        MemorySwap: memoryLimitBytes,
        NanoCpus: SANDBOX_CPU_NANOCPUS,
        PidsLimit: SANDBOX_PIDS_LIMIT,
        ReadonlyRootfs: true,
        Tmpfs: {
          "/tmp": "rw,noexec,nosuid,size=16m",
        },
        CapDrop: ["ALL"],
        SecurityOpt: ["no-new-privileges"],
        AutoRemove: false,
      },
    });

    // 1. Attach stdin hijack stream before container starts
    const stdinStream = await container.attach({
      stream: true,
      stdin: true,
      stdout: false,
      stderr: false,
      hijack: true,
    });
    stdinStream.on("error", () => {
      // Ignore stdin errors if container terminates quickly
    });

    await container.start();

    // 2. Start wall-clock timer after container has started
    const startTime = Date.now();

    timer = setTimeout(() => {
      timedOut = true;
      if (container) {
        container.kill().catch(() => {});
      }
    }, totalTimeoutMs);

    // 3. Set up log streaming for stdout & stderr demuxing
    logStream = (await container.logs({
      follow: true,
      stdout: true,
      stderr: true,
    })) as NodeJS.ReadableStream;

    const outStream = new PassThrough();
    const errStream = new PassThrough();

    let stdout = "";
    let stderr = "";
    let totalOutputBytes = 0;

    outStream.on("data", (chunk: Buffer) => {
      totalOutputBytes += chunk.length;
      if (totalOutputBytes > MAX_OUTPUT_BYTES) {
        if (!outputCapExceeded) {
          outputCapExceeded = true;
          if (container) {
            container.kill().catch(() => {});
          }
        }
      } else {
        stdout += chunk.toString("utf8");
      }
    });

    errStream.on("data", (chunk: Buffer) => {
      totalOutputBytes += chunk.length;
      if (totalOutputBytes > MAX_OUTPUT_BYTES) {
        if (!outputCapExceeded) {
          outputCapExceeded = true;
          if (container) {
            container.kill().catch(() => {});
          }
        }
      } else {
        stderr += chunk.toString("utf8");
      }
    });

    docker.modem.demuxStream(logStream, outStream, errStream);

    const logEndedPromise = new Promise<void>((resolve) => {
      if (!logStream) return resolve();
      logStream.on("end", () => resolve());
      logStream.on("close", () => resolve());
      logStream.on("error", () => resolve());
    });

    // 4. Send stdin input to container and close stdin
    if (options.stdin !== undefined) {
      stdinStream.write(options.stdin);
    }
    stdinStream.end();

    // 5. Wait for container to exit and log stream to complete
    const waitPromise = container.wait();

    // Wait for container exit; give logStream up to 500ms to flush after exit
    const waitResult = await waitPromise;
    await Promise.race([
      logEndedPromise,
      new Promise<void>((resolve) => setTimeout(resolve, 500)),
    ]);

    const executionTimeMs = Math.max(0, Date.now() - startTime);

    if (timer) {
      clearTimeout(timer);
      timer = null;
    }

    // 6. Inspect container state
    const inspectData = await container.inspect();

    // 7. Measure memory (best effort)
    let memoryKb = 0;
    try {
      const stats = (await container.stats({
        stream: false,
      })) as unknown as {
        memory_stats?: {
          usage?: number;
        };
      };
      if (stats?.memory_stats?.usage) {
        memoryKb = Math.round(stats.memory_stats.usage / 1024);
      }
    } catch {
      // Best effort, container may already be exited
    }

    // 8. Determine outcome
    let outcome: SandboxOutcome;

    if (timedOut) {
      outcome = "TIME_LIMIT_EXCEEDED";
    } else if (outputCapExceeded) {
      outcome = "OUTPUT_LIMIT_EXCEEDED";
    } else if (inspectData.State.OOMKilled) {
      outcome = "MEMORY_LIMIT_EXCEEDED";
    } else if (
      inspectData.State.ExitCode !== 0 ||
      waitResult.StatusCode !== 0
    ) {
      if (
        stderr.includes("JavaScript heap out of memory") ||
        stderr.includes("MemoryError")
      ) {
        outcome = "MEMORY_LIMIT_EXCEEDED";
      } else {
        outcome = "RUNTIME_ERROR";
      }
    } else {
      if (options.expectedOutput !== undefined) {
        outcome = compareOutput(stdout, options.expectedOutput)
          ? "ACCEPTED"
          : "WRONG_ANSWER";
      } else {
        outcome = "ACCEPTED";
      }
    }

    return {
      outcome,
      stdout: stdout.slice(0, MAX_OUTPUT_BYTES),
      stderr: stderr.slice(0, MAX_STDERR_CHARS),
      timeMs: executionTimeMs,
      memoryKb,
    };
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
    if (container) {
      try {
        await container.remove({ force: true });
      } catch {
        // Ignore errors during final container cleanup
      }
    }
  }
}
