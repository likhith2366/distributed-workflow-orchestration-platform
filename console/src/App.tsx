import { useCallback, useMemo, useState } from "react";
import { ApiError, createApi, type Api } from "./api";
import { Composer } from "./components/Composer";
import { MetricsBar } from "./components/MetricsBar";
import { RunDetail } from "./components/RunDetail";
import { RunList } from "./components/RunList";
import { TokenDialog } from "./components/TokenDialog";
import { TERMINAL, type Execution, type Metrics, type RunSummary, type WorkflowDefinition } from "./types";
import { usePolling } from "./usePolling";

const TOKEN_KEY = "conductor-token";
function readToken(): string {
  try {
    return sessionStorage.getItem(TOKEN_KEY) ?? "";
  } catch {
    return "";
  }
}

export function App({ apiFactory = createApi }: { apiFactory?: (token: string) => Api }) {
  const [token, setToken] = useState(readToken);
  const api = useMemo(() => apiFactory(token), [apiFactory, token]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [runs, setRuns] = useState<RunSummary[] | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<Execution | null>(null);
  const [error, setError] = useState<string>("");
  const [connected, setConnected] = useState<boolean | null>(null);
  const [dialog, setDialog] = useState<"compose" | "token" | null>(null);
  const [now, setNow] = useState(() => Date.now());

  const report = useCallback((e: unknown) => {
    if ((e as Error).name === "AbortError") return;
    setError(e instanceof ApiError && e.status === 401 ? "The coordinator requires an API token." : (e as Error).message);
  }, []);

  const refreshList = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const [m, r] = await Promise.all([api.metrics(signal), api.list(signal)]);
        setMetrics(m);
        setRuns(r);
        setConnected(true);
        setError("");
        setNow(Date.now());
        setSelected((current) => current ?? r[0]?.id ?? null);
      } catch (e) {
        if ((e as Error).name !== "AbortError") setConnected(false);
        report(e);
      }
    },
    [api, report],
  );
  const refreshDetail = useCallback(
    async (signal?: AbortSignal) => {
      if (!selected) return;
      try {
        const run = await api.get(selected, signal);
        setDetail(run);
        setNow(Date.now());
      } catch (e) {
        report(e);
      }
    },
    [api, selected, report],
  );

  usePolling(refreshList, 1500);
  // Live executions refresh faster; finished ones are immutable and need no polling.
  const live = detail === null || detail.id !== selected || !TERMINAL.has(detail.status);
  usePolling(refreshDetail, 1000, selected !== null && live);

  const visibleDetail = detail && detail.id === selected ? detail : null;

  const start = async (definition: WorkflowDefinition) => {
    const { id } = await api.start(definition, crypto.randomUUID());
    setDialog(null);
    setSelected(id);
    setDetail(null);
    await refreshList();
  };
  const mutate = async (action: () => Promise<Execution>) => {
    try {
      setDetail(await action());
      await refreshList();
    } catch (e) {
      report(e);
    }
  };

  return (
    <div className="shell">
      <aside>
        <div className="brand">
          <span className="brand-mark">C</span> conductor<span className="version">02</span>
        </div>
        <div className="nav-label">CONTROL PLANE</div>
        <a className="nav active" href="/" aria-current="page">
          ◈ &nbsp; Executions
        </a>
        <div className="aside-note">
          <span className="dot" /> {metrics?.engine.backend === "dynamodb" ? "DYNAMODB · SQS · S3" : "LOCAL DURABLE JOURNAL"}
          <p>
            Spring Boot · Java 21
            <br />
            Fenced worker leases
          </p>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <span className="eyebrow">WORKFLOW OPERATIONS</span>
            <h1>Every step accounted for.</h1>
            <p className="muted">Inspect execution, recover from failure, and trace what happened.</p>
          </div>
          <span className={"connection" + (connected === false ? " offline" : "")} role="status">
            {connected === null ? "Connecting" : connected ? `● Connected · ${metrics?.engine.backend ?? ""}` : "Disconnected"}
          </span>
        </header>
        <MetricsBar metrics={metrics} />
        <section className="toolbar">
          <div>
            <h2>
              Executions <span className="pill">LATEST 100</span>
            </h2>
            <p className="muted">Retail fulfillment, with failure built into the demonstration.</p>
          </div>
          <div className="actions">
            <button type="button" className="secondary" onClick={() => setDialog("token")}>
              API token
            </button>
            <button type="button" onClick={() => setDialog("compose")}>
              ＋ New execution
            </button>
          </div>
        </section>
        {error && (
          <p role="alert" className="error-banner">
            {error}
          </p>
        )}
        <div className="workspace">
          <RunList runs={runs} selected={selected} onSelect={setSelected} now={now} />
          {visibleDetail ? (
            <RunDetail
              run={visibleDetail}
              now={now}
              onApprove={(step) => mutate(() => api.signal(visibleDetail.id, step))}
              onCancel={() => mutate(() => api.cancel(visibleDetail.id))}
            />
          ) : (
            <section className="panel detail-panel">
              <div className="empty large">
                <span className="empty-icon">◈</span>
                <h2>The story of an execution</h2>
                <p>Select a run to inspect its steps, attempts, and fencing tokens.</p>
              </div>
            </section>
          )}
        </div>
        <footer>
          AT-LEAST-ONCE TASKS. IDEMPOTENT EFFECTS. <span>Local results are not AWS benchmarks.</span>
        </footer>
      </main>
      {dialog === "compose" && <Composer onClose={() => setDialog(null)} onSubmit={start} />}
      {dialog === "token" && (
        <TokenDialog
          token={token}
          onClose={() => setDialog(null)}
          onSave={(value) => {
            try {
              sessionStorage.setItem(TOKEN_KEY, value);
            } catch {
              // Private mode: the token lasts for this page only.
            }
            setToken(value);
            setDialog(null);
          }}
        />
      )}
    </div>
  );
}
