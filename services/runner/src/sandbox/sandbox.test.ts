import { describe, it, expect, afterAll } from "vitest";
import Docker from "dockerode";
import { runInSandbox } from "./runner";
import { compareOutput, normalizeOutput } from "./comparer";

const docker = new Docker();

describe("Sandbox Library", () => {
  describe("Output Comparison Rule", () => {
    it("normalises CRLF, trims trailing line whitespace, and ignores trailing blank lines", () => {
      const actual = "hello world   \r\n42  \r\n\r\n\r\n";
      const expected = "hello world\n42\n";
      expect(normalizeOutput(actual)).toBe("hello world\n42");
      expect(compareOutput(actual, expected)).toBe(true);
    });

    it("distinguishes non-matching outputs", () => {
      expect(compareOutput("hello", "world")).toBe(false);
      expect(compareOutput("1 2", "1  2")).toBe(false);
    });
  });

  describe.sequential("Code Execution & Security", () => {
    it(
      "prints expected output for a normal Python program",
      async () => {
        const code = `
import sys
line = sys.stdin.read().strip()
a, b = map(int, line.split())
print(a + b)
`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          stdin: "10 20\n",
          expectedOutput: "30\n",
          docker,
        });

        expect(result.outcome).toBe("ACCEPTED");
        expect(result.stdout.trim()).toBe("30");
      },
      15000
    );

    it(
      "prints expected output for a normal JavaScript program",
      async () => {
        const code = `
const fs = require('fs');
const line = fs.readFileSync(0, 'utf8').trim();
const [a, b] = line.split(/\\s+/).map(Number);
console.log(a + b);
`;
        const result = await runInSandbox({
          language: "JAVASCRIPT",
          code,
          stdin: "15 25\n",
          expectedOutput: "40\n",
          docker,
        });

        expect(result.outcome).toBe("ACCEPTED");
        expect(result.stdout.trim()).toBe("40");
      },
      15000
    );

    it(
      "returns WRONG_ANSWER when output does not match expected",
      async () => {
        const code = `print("wrong answer")`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          expectedOutput: "expected answer",
          docker,
        });

        expect(result.outcome).toBe("WRONG_ANSWER");
        expect(result.stdout.trim()).toBe("wrong answer");
      },
      15000
    );

    it(
      "infinite loop returns TIME_LIMIT_EXCEEDED within about limit + 500 ms and container is gone",
      async () => {
        const code = `
while True:
    pass
`;
        const timeLimitMs = 1000;
        const startupAllowanceMs = 500;
        const start = Date.now();

        const result = await runInSandbox({
          language: "PYTHON",
          code,
          timeLimitMs,
          startupAllowanceMs,
          docker,
        });

        const elapsed = Date.now() - start;
        expect(result.outcome).toBe("TIME_LIMIT_EXCEEDED");
        // Should terminate close to 1500ms, definitely well under 3500ms
        expect(elapsed).toBeLessThan(3500);
      },
      20000
    );

    it(
      "memory bomb in Python returns MEMORY_LIMIT_EXCEEDED",
      async () => {
        const code = `
x = ' ' * (500 * 1024 * 1024)
`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          memoryLimitMb: 64,
          docker,
        });

        expect(result.outcome).toBe("MEMORY_LIMIT_EXCEEDED");
      },
      20000
    );

    it(
      "memory bomb in JavaScript returns MEMORY_LIMIT_EXCEEDED",
      async () => {
        const code = `
const a = [];
while (true) {
  a.push(new Array(1000000));
}
`;
        const result = await runInSandbox({
          language: "JAVASCRIPT",
          code,
          timeLimitMs: 5000,
          memoryLimitMb: 64,
          docker,
        });

        expect(result.outcome).toBe("MEMORY_LIMIT_EXCEEDED");
      },
      25000
    );

    it(
      "fork bomb does not take down the host (pids limit holds) and returns within time limit",
      async () => {
        const code = `
import os
while True:
    try:
        os.fork()
    except OSError:
        break
print("pids limit held")
`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          timeLimitMs: 2000,
          startupAllowanceMs: 500,
          docker,
        });

        // PID limit of 64 holds, either returns RUNTIME_ERROR/ACCEPTED or TLE without crashing host
        expect([
          "ACCEPTED",
          "RUNTIME_ERROR",
          "TIME_LIMIT_EXCEEDED",
        ]).toContain(result.outcome);
      },
      20000
    );

    it(
      "network attempt in Python fails due to NetworkMode none",
      async () => {
        const code = `
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(1)
s.connect(('1.1.1.1', 80))
`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          docker,
        });

        expect(result.outcome).toBe("RUNTIME_ERROR");
      },
      15000
    );

    it(
      "network attempt in JavaScript fails due to NetworkMode none",
      async () => {
        const code = `
fetch('http://1.1.1.1:80').catch((err) => {
  process.exit(1);
});
`;
        const result = await runInSandbox({
          language: "JAVASCRIPT",
          code,
          docker,
        });

        expect(result.outcome).toBe("RUNTIME_ERROR");
      },
      15000
    );

    it(
      "writing a file to / fails due to ReadonlyRootfs",
      async () => {
        const code = `
with open('/exploit.txt', 'w') as f:
    f.write('failed')
`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          docker,
        });

        expect(result.outcome).toBe("RUNTIME_ERROR");
      },
      15000
    );

    it(
      "writing to /tmp is allowed, but /tmp is not executable (noexec)",
      async () => {
        // Step 1: Writing to /tmp is allowed
        const writeCode = `
with open('/tmp/test.txt', 'w') as f:
    f.write('hello from tmp')
with open('/tmp/test.txt', 'r') as f:
    print(f.read())
`;
        const writeResult = await runInSandbox({
          language: "PYTHON",
          code: writeCode,
          docker,
        });

        expect(writeResult.outcome).toBe("ACCEPTED");
        expect(writeResult.stdout.trim()).toBe("hello from tmp");

        // Step 2: Executing from /tmp fails because tmpfs has noexec
        const execCode = `
import os, stat
path = '/tmp/script.sh'
with open(path, 'w') as f:
    f.write('#!/bin/sh\\necho exploit\\n')
os.chmod(path, stat.S_IRWXU)
os.execv(path, [path])
`;
        const execResult = await runInSandbox({
          language: "PYTHON",
          code: execCode,
          docker,
        });

        expect(execResult.outcome).toBe("RUNTIME_ERROR");
      },
      20000
    );

    it(
      "printing 100 MB of output is killed at the cap and returns OUTPUT_LIMIT_EXCEEDED",
      async () => {
        const code = `
import sys
chunk = "A" * 1024 * 1024
for _ in range(100):
    sys.stdout.write(chunk)
    sys.stdout.flush()
`;
        const result = await runInSandbox({
          language: "PYTHON",
          code,
          docker,
        });

        expect(result.outcome).toBe("OUTPUT_LIMIT_EXCEEDED");
        expect(Buffer.byteLength(result.stdout, "utf8")).toBeLessThanOrEqual(
          65536
        );
      },
      20000
    );

    it(
      "reading os.environ / process.env does not reveal SUBMISSION_CODE",
      async () => {
        // Python check
        const pyCode = `
import os
assert 'SUBMISSION_CODE' not in os.environ, 'SUBMISSION_CODE found in os.environ'
print('ENV_CLEAN')
`;
        const pyResult = await runInSandbox({
          language: "PYTHON",
          code: pyCode,
          docker,
        });
        expect(pyResult.outcome).toBe("ACCEPTED");
        expect(pyResult.stdout.trim()).toBe("ENV_CLEAN");

        // JavaScript check
        const jsCode = `
if ('SUBMISSION_CODE' in process.env) {
  throw new Error('SUBMISSION_CODE found in process.env');
}
console.log('ENV_CLEAN');
`;
        const jsResult = await runInSandbox({
          language: "JAVASCRIPT",
          code: jsCode,
          docker,
        });
        expect(jsResult.outcome).toBe("ACCEPTED");
        expect(jsResult.stdout.trim()).toBe("ENV_CLEAN");
      },
      20000
    );

    it("rejects code larger than 64 KiB", async () => {
      const hugeCode = "x = 1\n".repeat(20000); // > 64 KiB
      await expect(
        runInSandbox({
          language: "PYTHON",
          code: hugeCode,
          docker,
        })
      ).rejects.toThrow("exceeds maximum allowed size of 64 KiB");
    });
  });

  afterAll(async () => {
    // Check residual containers
    const containers = await docker.listContainers({
      all: true,
      filters: {
        label: ["bytearena.submission"],
      },
    });
    expect(containers.length).toBe(0);
  });
});
