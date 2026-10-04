import * as grpc from "@grpc/grpc-js";
import { config, logger, loadJudgeProto, closeDb } from "@bytearena/shared";
import { submissionHandlers } from "./handlers/submission";
import { judgeHandlers } from "./handlers/judge";

let serverInstance: grpc.Server | null = null;

export function createGrpcServer(): grpc.Server {
  const proto = loadJudgeProto();
  const server = new grpc.Server({
    "grpc.max_receive_message_length": 64 * 1024 * 1024,
    "grpc.max_send_message_length": 64 * 1024 * 1024,
  });

  server.addService(
    proto.bytearena.v1.SubmissionService.service,
    submissionHandlers as unknown as grpc.UntypedServiceImplementation
  );

  server.addService(
    proto.bytearena.v1.JudgeService.service,
    judgeHandlers as unknown as grpc.UntypedServiceImplementation
  );

  return server;
}

export async function startServer(port = config.GRPC_PORT): Promise<grpc.Server> {
  const server = createGrpcServer();
  serverInstance = server;

  return new Promise<grpc.Server>((resolve, reject) => {
    const bindAddr = `0.0.0.0:${port}`;
    server.bindAsync(bindAddr, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
      if (err) {
        logger.error({ err, bindAddr }, "Failed to bind gRPC server");
        return reject(err);
      }
      logger.info({ port: boundPort }, "gRPC Submission & Judge service listening");
      resolve(server);
    });
  });
}

export async function stopServer(): Promise<void> {
  if (!serverInstance) return;

  const server = serverInstance;
  serverInstance = null;

  await new Promise<void>((resolve) => {
    server.tryShutdown((err) => {
      if (err) {
        logger.warn({ err }, "Graceful shutdown failed, forcing server shutdown");
        server.forceShutdown();
      }
      resolve();
    });
  });

  await closeDb();
  logger.info("Submission service shutdown complete");
}

function handleSignal(signal: string) {
  logger.info({ signal }, "Received shutdown signal");
  stopServer()
    .then(() => process.exit(0))
    .catch((err) => {
      logger.error({ err }, "Error during shutdown");
      process.exit(1);
    });
}

if (require.main === module) {
  process.on("SIGTERM", () => handleSignal("SIGTERM"));
  process.on("SIGINT", () => handleSignal("SIGINT"));

  startServer().catch((err) => {
    logger.fatal({ err }, "Fatal error starting gRPC server");
    process.exit(1);
  });
}
