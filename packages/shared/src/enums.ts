import { LanguageType, SubmissionStatusType, VerdictType } from "./events";

// Proto Enum strings (from proto-loader with --enums=String)
export type ProtoLanguage = "LANGUAGE_UNSPECIFIED" | "LANGUAGE_PYTHON" | "LANGUAGE_JAVASCRIPT";
export type ProtoSubmissionStatus =
  | "SUBMISSION_STATUS_UNSPECIFIED"
  | "STATUS_QUEUED"
  | "STATUS_JUDGING"
  | "STATUS_COMPLETED"
  | "STATUS_SYSTEM_ERROR";
export type ProtoVerdict =
  | "VERDICT_UNSPECIFIED"
  | "VERDICT_ACCEPTED"
  | "VERDICT_WRONG_ANSWER"
  | "VERDICT_RUNTIME_ERROR"
  | "VERDICT_TIME_LIMIT_EXCEEDED"
  | "VERDICT_MEMORY_LIMIT_EXCEEDED"
  | "VERDICT_OUTPUT_LIMIT_EXCEEDED"
  | "VERDICT_INTERNAL_ERROR";

export function toProtoLanguage(lang: LanguageType): ProtoLanguage {
  switch (lang) {
    case "PYTHON":
      return "LANGUAGE_PYTHON";
    case "JAVASCRIPT":
      return "LANGUAGE_JAVASCRIPT";
    default:
      return "LANGUAGE_UNSPECIFIED";
  }
}

export function fromProtoLanguage(protoLang: ProtoLanguage | string): LanguageType {
  switch (protoLang) {
    case "LANGUAGE_PYTHON":
      return "PYTHON";
    case "LANGUAGE_JAVASCRIPT":
      return "JAVASCRIPT";
    default:
      throw new Error(`Unsupported proto language: ${protoLang}`);
  }
}

export function toProtoStatus(status: SubmissionStatusType): ProtoSubmissionStatus {
  switch (status) {
    case "QUEUED":
      return "STATUS_QUEUED";
    case "JUDGING":
      return "STATUS_JUDGING";
    case "COMPLETED":
      return "STATUS_COMPLETED";
    case "SYSTEM_ERROR":
      return "STATUS_SYSTEM_ERROR";
    default:
      return "SUBMISSION_STATUS_UNSPECIFIED";
  }
}

export function fromProtoStatus(protoStatus: ProtoSubmissionStatus | string): SubmissionStatusType {
  switch (protoStatus) {
    case "STATUS_QUEUED":
      return "QUEUED";
    case "STATUS_JUDGING":
      return "JUDGING";
    case "STATUS_COMPLETED":
      return "COMPLETED";
    case "STATUS_SYSTEM_ERROR":
      return "SYSTEM_ERROR";
    default:
      throw new Error(`Unsupported proto status: ${protoStatus}`);
  }
}

export function toProtoVerdict(verdict: VerdictType | null | undefined): ProtoVerdict {
  if (!verdict) return "VERDICT_UNSPECIFIED";
  switch (verdict) {
    case "ACCEPTED":
      return "VERDICT_ACCEPTED";
    case "WRONG_ANSWER":
      return "VERDICT_WRONG_ANSWER";
    case "RUNTIME_ERROR":
      return "VERDICT_RUNTIME_ERROR";
    case "TIME_LIMIT_EXCEEDED":
      return "VERDICT_TIME_LIMIT_EXCEEDED";
    case "MEMORY_LIMIT_EXCEEDED":
      return "VERDICT_MEMORY_LIMIT_EXCEEDED";
    case "OUTPUT_LIMIT_EXCEEDED":
      return "VERDICT_OUTPUT_LIMIT_EXCEEDED";
    case "INTERNAL_ERROR":
      return "VERDICT_INTERNAL_ERROR";
    default:
      return "VERDICT_UNSPECIFIED";
  }
}

export function fromProtoVerdict(protoVerdict: ProtoVerdict | string): VerdictType {
  switch (protoVerdict) {
    case "VERDICT_ACCEPTED":
      return "ACCEPTED";
    case "VERDICT_WRONG_ANSWER":
      return "WRONG_ANSWER";
    case "VERDICT_RUNTIME_ERROR":
      return "RUNTIME_ERROR";
    case "VERDICT_TIME_LIMIT_EXCEEDED":
      return "TIME_LIMIT_EXCEEDED";
    case "VERDICT_MEMORY_LIMIT_EXCEEDED":
      return "MEMORY_LIMIT_EXCEEDED";
    case "VERDICT_OUTPUT_LIMIT_EXCEEDED":
      return "OUTPUT_LIMIT_EXCEEDED";
    case "VERDICT_INTERNAL_ERROR":
      return "INTERNAL_ERROR";
    default:
      throw new Error(`Unsupported proto verdict: ${protoVerdict}`);
  }
}
