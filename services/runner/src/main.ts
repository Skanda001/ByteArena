import { logger } from "@bytearena/shared";
import { RunnerWorker } from "./worker";

async function main(): Promise<void> {
  const worker = new RunnerWorker();

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "Received shutdown signal; stopping runner worker...");
    try {
      await worker.stop();
      process.exit(0);
    } catch (err: unknown) {
      logger.error({ err }, "Error during runner worker graceful shutdown");
      process.exit(1);
    }
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  try {
    await worker.start();
  } catch (err: unknown) {
    logger.fatal({ err }, "Fatal error starting runner worker");
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch((err) => {
    logger.fatal({ err }, "Unhandled error in runner worker main");
    process.exit(1);
  });
}

export { main };
