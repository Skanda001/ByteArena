import Docker from "dockerode";
import { logger } from "@bytearena/shared";

/**
 * Remove any leftover container labelled `bytearena.submission` that is older than olderThanMs.
 */
export async function cleanupOrphanContainers(
  docker: Docker,
  olderThanMs: number = 2 * 60 * 1000
): Promise<number> {
  try {
    const containers = await docker.listContainers({
      all: true,
      filters: {
        label: ["bytearena.submission"],
      },
    });

    const now = Date.now();
    let removedCount = 0;

    for (const info of containers) {
      const createdAtMs = info.Created * 1000;
      if (now - createdAtMs >= olderThanMs) {
        try {
          const container = docker.getContainer(info.Id);
          await container.remove({ force: true });
          removedCount++;
          logger.info(
            { containerId: info.Id, ageMs: now - createdAtMs },
            "Cleaned up orphan sandbox container"
          );
        } catch (err: unknown) {
          logger.warn(
            { containerId: info.Id, err },
            "Failed to remove orphan sandbox container"
          );
        }
      }
    }

    return removedCount;
  } catch (err: unknown) {
    logger.warn({ err }, "Failed to list orphan containers for cleanup");
    return 0;
  }
}
