import { describe, it, expect } from "vitest";
import { RunnerWorker } from "./worker";

describe("Runner Worker Unit Tests", () => {
  it("initializes cleanly with default options", () => {
    const worker = new RunnerWorker({
      clientId: "test-runner",
      groupId: "test-runner-group",
    });
    expect(worker).toBeDefined();
  });
});
