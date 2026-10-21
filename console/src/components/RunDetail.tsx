import { useState } from "react";
import { TERMINAL, type Execution, type Task } from "../types";
import { DagView } from "./DagView";
import { StatusBadge, relativeTime } from "./common";

interface Props {
  run: Execution;
  now: number;
  onApprove: (step: string) => Promise<void>;
  onCancel: () => Promise<void>;
}

function timing(task: Task, now: number): string | null {
  if (task.status === "RUNNING" && task.leaseUntil) {
    const ms = task.leaseUntil - now;
    return ms > 0 ? `lease ${(ms / 1000).toFixed(1)}s left` : "lease expired · awaiting reclaim";
  }
  if (task.status === "READY" && task.readyAt && task.readyAt > now) return `due ${relativeTime(task.readyAt, now)}`;
  if (task.status === "SUCCEEDED" && task.completedAt) return `done ${relativeTime(task.completedAt, now)}`;
  return null;
}

export function RunDetail({ run, now, onApprove, onCancel }: Props) {
  const [busy, setBusy] = useState<string | null>(null);
  const [focused, setFocused] = useState<string | null>(null);
  const act = async (key: string, action: () => Promise<void>) => {
    setBusy(key);
    try {
      await action();
    } finally {
      setBusy(null);
    }
  };
  const pick = (id: string) => {
    setFocused(id);
    document.getElementById(`step-${id}`)?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  };
  return (
    <section className="panel detail-panel" aria-label="Execution detail">
      <div className="detail-heading">
        <h2>
          <span>{run.definition.name}</span>
          <StatusBadge status={run.status} />
        </h2>
        <p className="mono">
          {run.id} · VERSION {run.definition.version} · STARTED {new Date(run.createdAt).toLocaleString()}
        </p>
        {!TERMINAL.has(run.status) && (
          <div className="actions">
            <button
              type="button"
              className="secondary"
              disabled={busy !== null}
              onClick={() => {
                if (
                  confirm(
                    "Stop scheduling this execution? Already-started external effects may still finish. Cancellation does not compensate them.",
                  )
                )
                  void act("cancel", onCancel);
              }}
            >
              Cancel execution
            </button>
          </div>
        )}
      </div>
      <DagView tasks={run.tasks} onPick={pick} />
      <ol className="steps">
        {Object.entries(run.tasks).map(([name, task]) => {
          const when = timing(task, now);
          return (
            <li
              key={name}
              id={`step-${name}`}
              className={`step step-${task.status.toLowerCase()}${focused === name ? " focused" : ""}`}
            >
              <div className="step-title">
                <span>
                  {task.compensation && <span className="undo-tag">UNDO</span>}
                  {name}
                </span>
                <StatusBadge status={task.status} />
              </div>
              <div className="step-meta">
                {task.spec.action ?? "echo"} · attempt {task.attempt}/{task.spec.maxAttempts ?? 3} · fence {task.fence}
                {task.owner && task.status === "RUNNING" ? ` · ${task.owner.slice(0, 13)}` : ""}
                {when ? ` · ${when}` : ""}
                {task.spec.dependsOn?.length ? ` · after ${task.spec.dependsOn.join(", ")}` : ""}
              </div>
              {task.error && <div className="step-error">{task.error}</div>}
              {task.result !== undefined && task.result !== null && (
                <details className="result">
                  <summary>Result{task.resultStoredIn === "s3" ? " (stored in S3)" : ""}</summary>
                  <pre>{JSON.stringify(task.result, null, 2)}</pre>
                </details>
              )}
              {task.status === "WAITING" && (
                <button type="button" disabled={busy !== null} onClick={() => void act(name, () => onApprove(name))}>
                  {busy === name ? "Approving…" : "Approve step →"}
                </button>
              )}
            </li>
          );
        })}
      </ol>
    </section>
  );
}
