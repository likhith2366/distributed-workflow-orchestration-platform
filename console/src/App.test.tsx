import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { App } from "./App";
import type { Api } from "./api";
import { createApi } from "./api";
import { parseDefinition } from "./components/Composer";
import { layout } from "./dag";
import type { Execution, Metrics, Task } from "./types";

const metrics: Metrics = {
  engine: { backend: "journal", executions: 1, active: 1, durableTransitions: 7, statuses: { RUNNING: 1 } },
  workers: { mode: "embedded", dispatchP99Ms: 12, dispatchSampleCount: 5 },
  ledgerEffects: 2,
};
const task = (status: Task["status"], extra: Partial<Task> = {}): Task => ({
  spec: { id: "x" },
  status,
  attempt: status === "PENDING" ? 0 : 1,
  fence: status === "PENDING" ? 0 : 1,
  ...extra,
});
const run: Execution = {
  id: "11111111-2222-3333-4444-555555555555",
  requestKey: "k",
  definition: { name: "retail-order-fulfillment", version: "1", steps: [] },
  createdAt: Date.now(),
  updatedAt: Date.now(),
  status: "RUNNING",
  tasks: {
    pay: task("SUCCEEDED", { spec: { id: "pay", action: "ledger" }, result: { amountCents: 2599 } }),
    review: task("WAITING", { spec: { id: "review", action: "approval", dependsOn: ["pay"] }, attempt: 0, fence: 0 }),
    ship: task("PENDING", { spec: { id: "ship", dependsOn: ["review"] } }),
  },
};

function fakeApi(overrides: Partial<Api> = {}): Api {
  return {
    metrics: vi.fn().mockResolvedValue(metrics),
    list: vi.fn().mockResolvedValue([{ id: run.id, name: run.definition.name, status: run.status, createdAt: run.createdAt, updatedAt: run.updatedAt }]),
    get: vi.fn().mockResolvedValue(run),
    start: vi.fn().mockResolvedValue({ id: "new-id" }),
    signal: vi.fn().mockResolvedValue({ ...run, tasks: { ...run.tasks, review: task("READY") } }),
    cancel: vi.fn().mockResolvedValue({ ...run, status: "CANCELLED" }),
    ...overrides,
  };
}

describe("console", () => {
  it("renders metrics, the run list and the selected execution", async () => {
    const api = fakeApi();
    render(<App apiFactory={() => api} />);
    expect(await screen.findByText("Connected · journal", { exact: false })).toBeTruthy();
    expect((await screen.findAllByText("retail-order-fulfillment")).length).toBeGreaterThan(0);
    expect(await screen.findByText("Approve step →")).toBeTruthy();
    expect(screen.getByRole("img", { name: "Workflow graph with 3 steps" })).toBeTruthy();
  });

  it("approves a waiting step through the signal API", async () => {
    const api = fakeApi();
    render(<App apiFactory={() => api} />);
    await userEvent.click(await screen.findByText("Approve step →"));
    await waitFor(() => expect(api.signal).toHaveBeenCalledWith(run.id, "review"));
  });

  it("submits a preset with a fresh idempotency key", async () => {
    const api = fakeApi();
    render(<App apiFactory={() => api} />);
    await userEvent.click(await screen.findByText("＋ New execution"));
    await userEvent.click(screen.getByText("Compensation demo"));
    await userEvent.click(screen.getByText("Start workflow →"));
    await waitFor(() => expect(api.start).toHaveBeenCalledTimes(1));
    const [definition, key] = (api.start as ReturnType<typeof vi.fn>).mock.calls[0]!;
    expect(definition.name).toBe("fulfillment-compensation");
    expect(key).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("explains a missing token instead of failing silently", async () => {
    const { ApiError } = await import("./api");
    const api = fakeApi({ metrics: vi.fn().mockRejectedValue(new ApiError("Bearer token required", 401)) });
    render(<App apiFactory={() => api} />);
    expect(await screen.findByText("The coordinator requires an API token.")).toBeTruthy();
    expect(screen.getByText("Disconnected")).toBeTruthy();
  });
});

describe("api client", () => {
  it("sends bearer and idempotency headers and surfaces server errors", async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: "Use 1..100 steps" }), { status: 400 }));
    const api = createApi("secret", fetcher as unknown as typeof fetch);
    await expect(api.start({ name: "a", version: "1", steps: [] }, "key-1")).rejects.toThrow("Use 1..100 steps");
    const [url, init] = fetcher.mock.calls[0]!;
    const headers = new Headers((init as RequestInit).headers);
    expect(url).toBe("/api/executions");
    expect(headers.get("Authorization")).toBe("Bearer secret");
    expect(headers.get("Idempotency-Key")).toBe("key-1");
  });
});

describe("definitions and layout", () => {
  it("rejects malformed definitions before calling the server", () => {
    expect(() => parseDefinition("{")).toThrow("Invalid JSON");
    expect(() => parseDefinition('{"name":"a","version":1,"steps":[{}]}')).toThrow("version");
    expect(parseDefinition('{"name":"a","version":"1","steps":[{"id":"x"}]}').name).toBe("a");
  });
  it("places each step one level after its deepest dependency", () => {
    const g = layout({
      a: task("SUCCEEDED", { spec: { id: "a" } }),
      b: task("READY", { spec: { id: "b", dependsOn: ["a"] } }),
      c: task("READY", { spec: { id: "c", dependsOn: ["a"] } }),
      join: task("PENDING", { spec: { id: "join", dependsOn: ["b", "c"] } }),
    });
    const level = Object.fromEntries(g.nodes.map((n) => [n.id, n.level]));
    expect(level).toEqual({ a: 0, b: 1, c: 1, join: 2 });
    expect(g.rows).toBe(2);
    expect(g.edges).toHaveLength(4);
  });
});
