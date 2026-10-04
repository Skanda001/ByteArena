import path from "path";
import fs from "fs";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import type { ProtoGrpcType } from "../generated/judge";
import type { SubmissionServiceClient } from "../generated/bytearena/v1/SubmissionService";
import type { JudgeServiceClient } from "../generated/bytearena/v1/JudgeService";

export function findProtoPath(): string {
  if (process.env.PROTO_PATH && fs.existsSync(process.env.PROTO_PATH)) {
    return process.env.PROTO_PATH;
  }

  // Walk up from current file to find proto/judge.proto
  let currentDir = __dirname;
  for (let i = 0; i < 5; i++) {
    const candidate = path.join(currentDir, "proto", "judge.proto");
    if (fs.existsSync(candidate)) {
      return candidate;
    }
    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) break;
    currentDir = parentDir;
  }

  // Fallback to process.cwd() / proto / judge.proto
  const cwdCandidate = path.join(process.cwd(), "proto", "judge.proto");
  if (fs.existsSync(cwdCandidate)) {
    return cwdCandidate;
  }

  throw new Error("Unable to locate proto/judge.proto");
}

export function loadJudgeProto(protoFilePath?: string): ProtoGrpcType {
  const filePath = protoFilePath || findProtoPath();
  const packageDefinition = protoLoader.loadSync(filePath, {
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  return grpc.loadPackageDefinition(packageDefinition) as unknown as ProtoGrpcType;
}

export function createSubmissionClient(
  address: string,
  credentials = grpc.credentials.createInsecure()
): SubmissionServiceClient {
  const proto = loadJudgeProto();
  return new proto.bytearena.v1.SubmissionService(address, credentials);
}

export function createJudgeClient(
  address: string,
  credentials = grpc.credentials.createInsecure()
): JudgeServiceClient {
  const proto = loadJudgeProto();
  return new proto.bytearena.v1.JudgeService(address, credentials);
}
