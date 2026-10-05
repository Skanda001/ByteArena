import { logger } from "@bytearena/shared";
import { GatewayServer } from "./server";

export * from "./server";
export * from "./rate-limiter";
export * from "./kafka-bridge";
export * from "./resolvers";
export * from "./validation";

async function main(): Promise<void> {
  const gateway = new GatewayServer();

  async function shutdown(): Promise<void> {
    logger.info("Graceful shutdown signal received by gateway...");
    await gateway.stop();
    process.exit(0);
  }

  process.on("SIGTERM", () => { void shutdown(); });
  process.on("SIGINT", () => { void shutdown(); });

  await gateway.start();
}

if (require.main === module) {
  main().catch((err) => {
    logger.fatal({ err }, "Gateway failed to start");
    process.exit(1);
  });
}
