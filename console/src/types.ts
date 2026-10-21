export type RunStatus =
  | "RUNNING"
  | "FAILING"
  | "COMPENSATING"
  | "SUCCEEDED"
  | "FAILED"
  | "COMPENSATED"
  | "COMPENSATION_FAILED"
  | "CANCELLED";

export type TaskStatus = "PENDING" | "READY" | "WAITING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";

export const TERMINAL: ReadonlySet<string> = new Set([
  "SUCCEEDED",
  "FAILED",
  "COMPENSATED",
  "COMPENSATION_FAILED",
  "CANCELLED",
]);

export interface StepSpec {
  id: string;
  action?: "echo" | "ledger" | "sleep" | "fail" | "wait" | "approval";
  dependsOn?: string[];
  compensate?: string;
  maxAttempts?: number;
  backoffMs?: number;
  delayMs?: number;
  durationMs?: number;
  failUntil?: number;
  value?: unknown;
}

export interface WorkflowDefinition {
  name: string;
  version: string;
  steps: StepSpec[];
}

export interface Task {
  spec: StepSpec;
  status: TaskStatus;
  attempt: number;
  fence: number;
  owner?: string;
  readyAt?: number;
  leaseUntil?: number;
  claimedAt?: number;
  completedAt?: number;
  error?: string;
  result?: unknown;
  resultStoredIn?: "s3";
  compensation?: boolean;
}

export interface Execution {
  id: string;
  requestKey: string;
  definition: WorkflowDefinition;
  createdAt: number;
  updatedAt: number;
  status: RunStatus;
  tasks: Record<string, Task>;
}

export interface RunSummary {
  id: string;
  name: string;
  status: RunStatus;
  createdAt: number;
  updatedAt: number;
}

export interface Metrics {
  engine: {
    backend: "journal" | "dynamodb";
    executions: number;
    active: number;
    durableTransitions: number;
    statuses: Record<string, number>;
    approximate?: boolean;
  };
  workers: {
    mode: string;
    completions?: number;
    fenceRejections?: number;
    dispatchP50Ms?: number;
    dispatchP99Ms?: number;
    dispatchSampleCount?: number;
  };
  ledgerEffects: number;
}
