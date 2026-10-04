import pino from "pino";
import { config } from "./config";

export const logger = pino({
  level: config.LOG_LEVEL,
  base: undefined, // Remove pid and hostname for cleaner JSON logs
  timestamp: pino.stdTimeFunctions.isoTime,
});

export function createServiceLogger(serviceName: string, extra: Record<string, unknown> = {}) {
  return logger.child({ service: serviceName, ...extra });
}

export type Logger = typeof logger;
