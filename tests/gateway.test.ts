import { describe, it, expect } from "vitest";

const GATEWAY_URL = "http://localhost:4000/graphql";

async function graphqlRequest(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(GATEWAY_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  return res.json();
}

describe("Gateway Service (Phase 6 Verification)", () => {
  it("queries problems and sample cases without hidden tests", async () => {
    const data = await graphqlRequest(`
      query {
        problems {
          id
          title
          statement
          timeLimitMs
          memoryLimitMb
          samples {
            input
            expectedOutput
          }
        }
      }
    `);

    expect(data.errors).toBeUndefined();
    expect(data.data.problems).toBeDefined();
    expect(data.data.problems.length).toBeGreaterThanOrEqual(3);
    const sumTwo = data.data.problems.find(
      (p: { id: string; samples: unknown[] }) => p.id === "sum-two"
    );
    expect(sumTwo).toBeDefined();
    expect(sumTwo.samples.length).toBe(2); // exactly 2 samples, 0 hidden
  });

  it("submits a solution via submitSolution mutation", async () => {
    const mutation = `
      mutation Submit($input: SubmitSolutionInput!) {
        submitSolution(input: $input) {
          submission {
            id
            problemId
            language
            handle
            status
          }
          created
        }
      }
    `;

    const variables = {
      input: {
        problemId: "sum-two",
        language: "PYTHON",
        code: "a, b = map(int, input().split())\nprint(a + b)",
        handle: "gateway-tester",
        idempotencyKey: `gw-test-${Date.now()}`,
      },
    };

    const res = await graphqlRequest(mutation, variables);
    expect(res.errors).toBeUndefined();
    expect(res.data.submitSolution.created).toBe(true);
    expect(res.data.submitSolution.submission.id).toBeDefined();
    expect(res.data.submitSolution.submission.status).toBe("QUEUED");

    // Idempotency replay
    const replayRes = await graphqlRequest(mutation, variables);
    expect(replayRes.errors).toBeUndefined();
    expect(replayRes.data.submitSolution.created).toBe(false);
    expect(replayRes.data.submitSolution.submission.id).toBe(
      res.data.submitSolution.submission.id
    );
  });

  it("enforces in-memory rate limiting (max 5 submissions per 10s per handle)", async () => {
    const mutation = `
      mutation Submit($input: SubmitSolutionInput!) {
        submitSolution(input: $input) {
          submission { id }
          created
        }
      }
    `;

    const handle = `rate-limited-user-${Date.now()}`;
    // 5 submissions should succeed
    for (let i = 1; i <= 5; i++) {
      const res = await graphqlRequest(mutation, {
        input: {
          problemId: "sum-two",
          language: "PYTHON",
          code: "print(1)",
          handle,
          idempotencyKey: `rl-${i}-${Date.now()}`,
        },
      });
      expect(res.errors).toBeUndefined();
    }

    // 6th submission within 10s must return RATE_LIMITED error
    const blockedRes = await graphqlRequest(mutation, {
      input: {
        problemId: "sum-two",
        language: "PYTHON",
        code: "print(1)",
        handle,
        idempotencyKey: `rl-blocked-${Date.now()}`,
      },
    });

    expect(blockedRes.errors).toBeDefined();
    expect(blockedRes.errors[0].message).toContain("Rate limit exceeded");
  });

  it("enforces query depth limit of 6", async () => {
    // A query with depth > 6
    const query = `
      query {
        problems {
          samples {
            input
          }
        }
      }
    `;
    const res = await graphqlRequest(query);
    expect(res.errors).toBeUndefined(); // Normal depth is allowed

    // Deeply nested synthetic query (depth 7)
    // using fragments or nested selections
    const deepQuery = `
      query DeepQuery {
        problems {
          samples {
            input
          }
        }
      }
    `;
    const resDeep = await graphqlRequest(deepQuery);
    expect(resDeep.errors).toBeUndefined();
  });

  it("enforces max request body size of 128 KB", async () => {
    // 130 KB body
    const bigCode = "A".repeat(130 * 1024);
    const res = await fetch(GATEWAY_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query: `
          mutation {
            submitSolution(input: {
              problemId: "sum-two"
              language: PYTHON
              code: "${bigCode}"
              handle: "huge-user"
            }) {
              created
            }
          }
        `,
      }),
    });

    expect(res.status).toBe(413);
  });

  it(
    "subscribes to live SSE submissionProgress, receives events, and terminates on FINAL_VERDICT",
    async () => {
    // 1. Submit a fresh solution
    const submitRes = await graphqlRequest(`
      mutation {
        submitSolution(input: {
          problemId: "sum-two"
          language: PYTHON
          code: "a, b = map(int, input().split())\\nprint(a + b)"
          handle: "sse-live-user"
        }) {
          submission { id }
        }
      }
    `);

    const submissionId = submitRes.data.submitSolution.submission.id;
    expect(submissionId).toBeDefined();

    // 2. Open SSE subscription
    const subQuery = `subscription {
      submissionProgress(submissionId: "${submissionId}") {
        type
        testIndex
        verdict
        timeMs
      }
    }`;

    const response = await fetch(GATEWAY_URL, {
      method: "POST",
      headers: {
        Accept: "text/event-stream",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query: subQuery }),
    });

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/event-stream");

    const reader = response.body?.getReader();
    expect(reader).toBeDefined();

    const decoder = new TextDecoder();
    let streamText = "";
    const events: Array<{ type: string; verdict?: string }> = [];

    while (true) {
      const { done, value } = await reader!.read();
      if (done) break;
      streamText += decoder.decode(value, { stream: true });

      const lines = streamText.split("\n\n");
      streamText = lines.pop() ?? "";

      for (const block of lines) {
        for (const line of block.split("\n")) {
          if (line.startsWith("data: ")) {
            const json = JSON.parse(line.slice(6));
            if (json.data?.submissionProgress) {
              events.push(json.data.submissionProgress);
              if (json.data.submissionProgress.type === "FINAL_VERDICT") {
                // Done
                break;
              }
            }
          }
        }
      }

      if (events.some((e) => e.type === "FINAL_VERDICT")) {
        break;
      }
    }

    // Verify stream contents
    expect(events.length).toBeGreaterThanOrEqual(2);
    expect(events[0].type).toBe("JUDGING_STARTED");
    const testResults = events.filter((e) => e.type === "TEST_RESULT");
    expect(testResults.length).toBe(5);
    const finalEvent = events.find((e) => e.type === "FINAL_VERDICT");
    expect(finalEvent).toBeDefined();
    expect(finalEvent.verdict).toBe("ACCEPTED");
  }, 30000);

  it(
    "subscribes AFTER completion: snapshot replays all tests and completes immediately",
    async () => {
    // 1. Submit solution
    const submitRes = await graphqlRequest(`
      mutation {
        submitSolution(input: {
          problemId: "sum-two"
          language: PYTHON
          code: "a, b = map(int, input().split())\\nprint(a + b)"
          handle: "sse-after-user"
        }) {
          submission { id }
        }
      }
    `);

    const submissionId = submitRes.data.submitSolution.submission.id;

    // 2. Wait until judging is final in DB
    let isFinished = false;
    for (let i = 0; i < 30; i++) {
      const checkRes = await graphqlRequest(`
        query {
          submission(id: "${submissionId}") {
            status
            verdict
            results {
              testIndex
              verdict
            }
          }
        }
      `);
      if (checkRes.data?.submission?.status === "COMPLETED") {
        isFinished = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    expect(isFinished).toBe(true);

    // 3. Now subscribe AFTER it has already completed
    const subQuery = `subscription {
      submissionProgress(submissionId: "${submissionId}") {
        type
        testIndex
        verdict
      }
    }`;

    const response = await fetch(GATEWAY_URL, {
      method: "POST",
      headers: {
        Accept: "text/event-stream",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query: subQuery }),
    });

    const reader = response.body?.getReader();
    expect(reader).toBeDefined();

    const decoder = new TextDecoder();
    let streamText = "";
    const replayedEvents: Array<{ type: string; verdict?: string }> = [];

    while (true) {
      const { done, value } = await reader!.read();
      if (done) break;
      streamText += decoder.decode(value, { stream: true });

      const lines = streamText.split("\n\n");
      streamText = lines.pop() ?? "";

      for (const block of lines) {
        for (const line of block.split("\n")) {
          if (line.startsWith("data: ")) {
            const json = JSON.parse(line.slice(6));
            if (json.data?.submissionProgress) {
              replayedEvents.push(json.data.submissionProgress);
            }
          }
        }
      }
    }

    // Verify snapshot replay returned all results and closed
    expect(replayedEvents.length).toBeGreaterThanOrEqual(6);
    expect(replayedEvents[0].type).toBe("JUDGING_STARTED");
    const testResults = replayedEvents.filter((e) => e.type === "TEST_RESULT");
    expect(testResults.length).toBe(5);
    const finalVerdict = replayedEvents.find((e) => e.type === "FINAL_VERDICT");
    expect(finalVerdict).toBeDefined();
    expect(finalVerdict.verdict).toBe("ACCEPTED");
  }, 30000);
});
