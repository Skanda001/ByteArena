import dotenv from "dotenv";
import { z } from "zod";

// Load .env if present
dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  POSTGRES_USER: z.string().default("bytearena"),
  POSTGRES_PASSWORD: z.string().default("bytearena"),
  POSTGRES_DB: z.string().default("bytearena"),
  DATABASE_URL: z.string().default("postgres://bytearena:bytearena@localhost:5432/bytearena"),
  KAFKA_BROKERS: z.string().default("localhost:29092"),
  SUBMISSION_GRPC_ADDR: z.string().default("localhost:50051"),
  GRPC_PORT: z.coerce.number().default(50051),
  GATEWAY_PORT: z.coerce.number().default(4000),
  LOG_LEVEL: z
    .enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"])
    .default("info"),
  RUNNER_SESSION_TIMEOUT_MS: z.coerce.number().default(30000),
  SANDBOX_STARTUP_ALLOWANCE_MS: z.coerce.number().default(500),
  SANDBOX_OUTPUT_CAP_BYTES: z.coerce.number().default(65536),
  DOCKER_GID: z.coerce.number().default(999),
});

const parsedEnv = envSchema.safeParse(process.env);

if (!parsedEnv.success) {
  // Print human-readable validation errors on startup
  console.error("Configuration validation failed:", parsedEnv.error.format());
  throw new Error("Invalid application configuration");
}

export const config = {
  ...parsedEnv.data,
  kafkaBrokersArray: parsedEnv.data.KAFKA_BROKERS.split(",").map((b) => b.trim()),
};

export type Config = typeof config;
