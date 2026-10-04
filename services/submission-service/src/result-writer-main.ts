import { config, logger, closeDb } from "@bytearena/shared";
import { ResultWriter } from "./result-writer";

const writer = new ResultWriter({
  clientId: "result-writer",
  groupId: "result-writer-group",
  sessionTimeout: config.RUNNER_SESSION_TIMEOUT_MS,
});

async function main(): Promise<void> {
  await writer.start();
}

async function shutdown(): Promise<void> {
  logger.info("SIGTERM received, shutting down result writer...");
  await writer.stop();
  await closeDb();
  process.exit(0);
}

process.on("SIGTERM", () => { void shutdown(); });
process.on("SIGINT", () => { void shutdown(); });

main().catch((err) => {
  logger.error({ err }, "Result writer crashed");
  process.exit(1);
});
