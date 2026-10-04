import { logger } from "@bytearena/shared";

export function initGateway(): void {
  logger.info("Gateway service scaffold initialized");
}

if (require.main === module) {
  initGateway();
}
