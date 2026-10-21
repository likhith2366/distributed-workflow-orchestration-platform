import type { RunSummary } from "../types";
import { StatusBadge, relativeTime } from "./common";

interface Props {
  runs: RunSummary[] | null;
  selected: string | null;
  onSelect: (id: string) => void;
  now: number;
}

export function RunList({ runs, selected, onSelect, now }: Props) {
  return (
    <section className="panel list-panel" aria-label="Executions">
      <div className="panel-header">
        EXECUTION / STARTED <span>STATUS</span>
      </div>
      {runs === null ? (
        <div className="empty">Loading executions…</div>
      ) : runs.length === 0 ? (
        <div className="empty">No executions yet. Launch a workflow to explore its lifecycle.</div>
      ) : (
        <ul className="runs">
          {runs.map((run) => (
            <li key={run.id}>
              <button
                type="button"
                className={"run" + (run.id === selected ? " selected" : "")}
                aria-pressed={run.id === selected}
                onClick={() => onSelect(run.id)}
              >
                <span>
                  <b>{run.name}</b>
                  <small title={new Date(run.createdAt).toLocaleString()}>
                    {run.id.slice(0, 8)} · {relativeTime(run.createdAt, now)}
                  </small>
                </span>
                <StatusBadge status={run.status} />
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
