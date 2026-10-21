import type { Task } from "./types";

export interface DagNode {
  id: string;
  level: number;
  row: number;
  task: Task;
}
export interface DagLayout {
  nodes: DagNode[];
  edges: { from: string; to: string }[];
  levels: number;
  rows: number;
}

/** Longest-path layering: a node sits one column right of its deepest dependency. */
export function layout(tasks: Record<string, Task>): DagLayout {
  const level = new Map<string, number>();
  const visiting = new Set<string>();
  const depth = (id: string): number => {
    const known = level.get(id);
    if (known !== undefined) return known;
    if (visiting.has(id)) return 0; // Defensive: the engine rejects cycles.
    visiting.add(id);
    const deps = (tasks[id]?.spec.dependsOn ?? []).filter((d) => d in tasks);
    const value = deps.length ? Math.max(...deps.map(depth)) + 1 : 0;
    visiting.delete(id);
    level.set(id, value);
    return value;
  };
  const ids = Object.keys(tasks);
  ids.forEach(depth);
  const perLevel = new Map<number, number>();
  const nodes: DagNode[] = ids.map((id) => {
    const l = level.get(id) ?? 0;
    const row = perLevel.get(l) ?? 0;
    perLevel.set(l, row + 1);
    return { id, level: l, row, task: tasks[id]! };
  });
  const edges = ids.flatMap((id) =>
    (tasks[id]?.spec.dependsOn ?? []).filter((d) => d in tasks).map((from) => ({ from, to: id })),
  );
  return {
    nodes,
    edges,
    levels: Math.max(0, ...nodes.map((n) => n.level)) + 1,
    rows: Math.max(1, ...perLevel.values()),
  };
}
