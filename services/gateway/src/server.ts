import http from "http";
import fs from "fs";
import path from "path";
import { createYoga, createSchema } from "graphql-yoga";
import {
  config,
  logger,
  createSubmissionClient,
  SubmissionServiceClient,
} from "@bytearena/shared";
import { resolvers, ResolverContext } from "./resolvers";
import { RateLimiter } from "./rate-limiter";
import { KafkaBridge } from "./kafka-bridge";
import { depthLimit } from "./validation";

export function loadSchemaSource(): string {
  const candidates = [
    path.resolve(__dirname, "../../../schema/schema.graphql"),
    path.resolve(__dirname, "../../schema/schema.graphql"),
    path.resolve(__dirname, "../schema/schema.graphql"),
    path.resolve(process.cwd(), "schema/schema.graphql"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return fs.readFileSync(candidate, "utf8");
    }
  }

  throw new Error(`schema/schema.graphql not found. Tried paths: ${candidates.join(", ")}`);
}

export interface GatewayServerOptions {
  port?: number;
  grpcAddr?: string;
  kafkaGroupId?: string;
  rateLimitMax?: number;
  rateLimitWindowMs?: number;
  maxDepth?: number;
  maxBodyBytes?: number;
}

export class GatewayServer {
  private server: http.Server | null = null;
  private submissionClient: SubmissionServiceClient;
  private rateLimiter: RateLimiter;
  private kafkaBridge: KafkaBridge;
  private port: number;
  private maxDepth: number;
  private maxBodyBytes: number;
  private isRunning = false;

  constructor(options: GatewayServerOptions = {}) {
    this.port = options.port ?? config.GATEWAY_PORT;
    this.maxDepth = options.maxDepth ?? 6;
    this.maxBodyBytes = options.maxBodyBytes ?? 128 * 1024; // 128 KiB
    const grpcAddr = options.grpcAddr ?? config.SUBMISSION_GRPC_ADDR;

    this.submissionClient = createSubmissionClient(grpcAddr);
    this.rateLimiter = new RateLimiter(
      options.rateLimitMax ?? 5,
      options.rateLimitWindowMs ?? 10000
    );
    this.kafkaBridge = new KafkaBridge({
      groupId: options.kafkaGroupId,
    });
  }

  createYogaApp() {
    const typeDefs = loadSchemaSource();
    const schema = createSchema<ResolverContext>({
      typeDefs,
      resolvers,
    });

    const maxDepth = this.maxDepth;
    const maxBodyBytes = this.maxBodyBytes;

    const yoga = createYoga<ResolverContext>({
      schema,
      context: () => ({
        submissionClient: this.submissionClient,
        rateLimiter: this.rateLimiter,
        kafkaBridge: this.kafkaBridge,
      }),
      plugins: [
        // 1. Query depth limit validation rule (max depth = 6)
        {
          onValidate({ addValidationRule }: { addValidationRule: (rule: unknown) => void }) {
            addValidationRule(depthLimit(maxDepth));
          },
        },
        // 2. Request body size limit check (max body size = 128 KiB)
        {
          onRequest({ request, fetchAPI, endResponse }) {
            const cl = request.headers.get("content-length");
            if (cl && parseInt(cl, 10) > maxBodyBytes) {
              endResponse(
                new fetchAPI.Response(
                  JSON.stringify({
                    errors: [
                      {
                        message: `Request body exceeds maximum allowed size of ${maxBodyBytes} bytes (128 KB).`,
                      },
                    ],
                  }),
                  {
                    status: 413,
                    headers: { "content-type": "application/json" },
                  }
                )
              );
            }
          },
        },
      ],
      graphiql: true,
    });

    return yoga;
  }

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    // Start Kafka bridge to receive live submission results
    await this.kafkaBridge.start();

    const yoga = this.createYogaApp();
    this.server = http.createServer((req, res) => {
      if (req.url === "/" || req.url === "") {
        res.writeHead(302, { Location: "/graphql" });
        res.end();
        return;
      }
      yoga(req, res);
    });

    await new Promise<void>((resolve, reject) => {
      this.server!.listen(this.port, () => {
        logger.info(
          { port: this.port, endpoint: `http://localhost:${this.port}/graphql` },
          "Gateway GraphQL server started"
        );
        resolve();
      });
      this.server!.once("error", reject);
    });
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    await this.kafkaBridge.stop();

    if (this.server) {
      await new Promise<void>((resolve) => {
        this.server!.close(() => resolve());
      });
      this.server = null;
    }

    logger.info("Gateway server stopped");
  }

  getRateLimiter(): RateLimiter {
    return this.rateLimiter;
  }

  getKafkaBridge(): KafkaBridge {
    return this.kafkaBridge;
  }
}
