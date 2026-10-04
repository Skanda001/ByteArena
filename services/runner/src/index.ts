import { logger } from "@bytearena/shared";
import Docker from "dockerode";
import { cleanupOrphanContainers } from "./sandbox";

export * from "./sandbox";

export async function initRunner(): Promise<void> {
  logger.info("Runner service scaffold initialized");
  const docker = new Docker();
  const cleaned = await cleanupOrphanContainers(docker);
  if (cleaned > 0) {
    logger.info({ cleaned }, "Cleaned up orphan sandbox containers on startup");
  }
}

if (require.main === module) {
  initRunner().catch((err) => {
    logger.error({ err }, "Failed to initialize runner service");
    process.exit(1);
  });
}
