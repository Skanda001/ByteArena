async function main() {
  console.log("=== ByteArena GraphQL Gateway Demo ===");
  // 1. Submit solution
  const submitResponse = await fetch("http://localhost:4000/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `
        mutation {
          submitSolution(input: {
            problemId: "sum-two"
            language: PYTHON
            code: "a, b = map(int, input().split())\\nprint(a + b)"
            handle: "sse-demo-user"
          }) {
            submission {
              id
              status
            }
          }
        }
      `,
    }),
  });

  interface SubmitResult {
    data: {
      submitSolution: {
        submission: {
          id: string;
          status: string;
        };
      };
    };
  }
  const submitJson = (await submitResponse.json()) as SubmitResult;
  const submissionId = submitJson.data.submitSolution.submission.id;
  console.log(`Created submission: ${submissionId}`);

  // 2. Stream SSE subscription
  console.log("Opening SSE subscription stream for submissionProgress...\n");
  const sseResponse = await fetch("http://localhost:4000/graphql", {
    method: "POST",
    headers: {
      Accept: "text/event-stream",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `
        subscription {
          submissionProgress(submissionId: "${submissionId}") {
            submissionId
            type
            totalTests
            testIndex
            verdict
            timeMs
            isSample
          }
        }
      `,
    }),
  });

  const reader = sseResponse.body?.getReader();
  if (!reader) throw new Error("No readable stream in response body");

  const decoder = new TextDecoder();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    process.stdout.write(decoder.decode(value));
  }
  console.log("\nSubscription finished and stream closed cleanly.");
}

main().catch(console.error);
