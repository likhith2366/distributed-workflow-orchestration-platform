import { useMemo } from "react";
import { layout } from "../dag";
import type { Task } from "../types";

const W = 148,
  H = 40,
  GX = 44,
  GY = 14,
  PAD = 8;

/** Dependency graph of the execution, including compensation steps, coloured by task status. */
export function DagView({ tasks, onPick }: { tasks: Record<string, Task>; onPick: (id: string) => void }) {
  const graph = useMemo(() => layout(tasks), [tasks]);
  const pos = new Map(graph.nodes.map((n) => [n.id, { x: PAD + n.level * (W + GX), y: PAD + n.row * (H + GY) }]));
  const width = PAD * 2 + graph.levels * W + (graph.levels - 1) * GX;
  const height = PAD * 2 + graph.rows * H + (graph.rows - 1) * GY;
  return (
    <div className="dag" role="img" aria-label={`Workflow graph with ${graph.nodes.length} steps`}>
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
        {graph.edges.map(({ from, to }) => {
          const a = pos.get(from)!,
            b = pos.get(to)!;
          const x1 = a.x + W,
            y1 = a.y + H / 2,
            x2 = b.x,
            y2 = b.y + H / 2,
            mx = (x1 + x2) / 2;
          return <path key={from + ">" + to} className="edge" d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`} />;
        })}
        {graph.nodes.map((n) => {
          const p = pos.get(n.id)!;
          const label = n.id.length > 18 ? n.id.slice(0, 17) + "…" : n.id;
          return (
            <g
              key={n.id}
              className={`node node-${n.task.status.toLowerCase()}${n.task.compensation ? " node-undo" : ""}`}
              transform={`translate(${p.x},${p.y})`}
              onClick={() => onPick(n.id)}
            >
              <title>{`${n.id}: ${n.task.status}`}</title>
              <rect width={W} height={H} rx={7} />
              <circle cx={14} cy={H / 2} r={4} />
              <text x={26} y={H / 2 + 4}>
                {label}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
