import Docker from "dockerode";
import { LanguageType, VerdictType } from "@bytearena/shared";

export type SandboxOutcome = VerdictType;

export interface RunInSandboxOptions {
  submissionId?: string;
  language: LanguageType;
  code: string;
  stdin?: string;
  expectedOutput?: string;
  timeLimitMs?: number;
  memoryLimitMb?: number;
  startupAllowanceMs?: number;
  docker?: Docker;
}

export interface SandboxResult {
  outcome: SandboxOutcome;
  stdout: string;
  stderr: string;
  timeMs: number;
  memoryKb: number;
}
