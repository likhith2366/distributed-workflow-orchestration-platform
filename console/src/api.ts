import type { Execution, Metrics, RunSummary, WorkflowDefinition } from "./types";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

export interface Api {
  metrics(signal?: AbortSignal): Promise<Metrics>;
  list(signal?: AbortSignal): Promise<RunSummary[]>;
  get(id: string, signal?: AbortSignal): Promise<Execution>;
  start(definition: WorkflowDefinition, idempotencyKey: string): Promise<{ id: string }>;
  signal(id: string, step: string): Promise<Execution>;
  cancel(id: string): Promise<Execution>;
}

export function createApi(token: string, fetcher: typeof fetch = (...args) => fetch(...args)): Api {
  async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set("Content-Type", "application/json");
    if (token) headers.set("Authorization", `Bearer ${token}`);
    const response = await fetcher(`/api${path}`, { ...init, headers });
    const text = await response.text();
    let body: unknown = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      throw new ApiError(`Unexpected response (HTTP ${response.status})`, response.status);
    }
    if (!response.ok) {
      const message = (body as { error?: string } | null)?.error ?? `Request failed (HTTP ${response.status})`;
      throw new ApiError(message, response.status);
    }
    return body as T;
  }
  const post = <T>(path: string, body: unknown, headers?: HeadersInit) =>
    call<T>(path, { method: "POST", body: JSON.stringify(body), headers });
  return {
    metrics: (signal) => call("/metrics", { signal }),
    list: (signal) => call("/executions?limit=100", { signal }),
    get: (id, signal) => call(`/executions/${encodeURIComponent(id)}`, { signal }),
    start: (definition, key) => post("/executions", definition, { "Idempotency-Key": key }),
    signal: (id, step) => post(`/executions/${encodeURIComponent(id)}/signal`, { step }),
    cancel: (id) => post(`/executions/${encodeURIComponent(id)}/cancel`, {}),
  };
}
