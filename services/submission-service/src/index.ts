import { logger } from "@bytearena/shared";

export function initSubmissionService(): void {
  logger.info("Submission service scaffold initialized");
}

if (require.main === module) {
  initSubmissionService();
}
