import type { Metrics } from "../types";

export function MetricsBar({ metrics }: { metrics: Metrics | null }) {
  const fmt = (n: number | undefined) => (n === undefined ? "—" : n.toLocaleString());
  const approx = metrics?.engine.approximate ? "≈ " : "";
  return (
    <section className="metrics" aria-label="Metrics">
      <article>
        <span className="label">Total executions</span>
        <strong>{approx + fmt(metrics?.engine.executions)}</strong>
        <small>Retained in durable history</small>
      </article>
      <article>
        <span className="label">In progress</span>
        <strong>{approx + fmt(metrics?.engine.active)}</strong>
        <small>Running, waiting, or compensating</small>
      </article>
      <article>
        <span className="label">Durable transitions</span>
        <strong>{approx + fmt(metrics?.engine.durableTransitions)}</strong>
        <small>{metrics?.engine.backend === "dynamodb" ? "DynamoDB transactions" : "Acknowledged after disk flush"}</small>
      </article>
      <article>
        <span className="label">
          Dispatch p99 <span className="unit">ms</span>
        </span>
        <strong>{fmt(metrics?.workers.dispatchP99Ms)}</strong>
        <small>
          {metrics?.workers.mode === "embedded" || metrics?.workers.mode === "sqs"
            ? `Last ${fmt(metrics.workers.dispatchSampleCount)} claims in this process`
            : "Measured by workers in other processes"}
        </small>
      </article>
    </section>
  );
}
