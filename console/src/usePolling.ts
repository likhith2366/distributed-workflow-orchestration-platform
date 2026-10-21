import { useEffect, useRef } from "react";

/**
 * Runs `task` now and every `intervalMs`, never overlapping, aborting in-flight work on cleanup,
 * and pausing while the tab is hidden so idle consoles do not load the coordinator.
 */
export function usePolling(task: (signal: AbortSignal) => Promise<void>, intervalMs: number, enabled = true): void {
  const latest = useRef(task);
  latest.current = task;
  useEffect(() => {
    if (!enabled) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;
    const controller = new AbortController();
    const tick = async () => {
      if (stopped) return;
      if (document.visibilityState !== "hidden") {
        try {
          await latest.current(controller.signal);
        } catch {
          // Callers report their own errors; polling continues.
        }
      }
      if (!stopped) timer = setTimeout(tick, intervalMs);
    };
    const onVisible = () => {
      if (document.visibilityState === "visible") {
        clearTimeout(timer);
        void tick();
      }
    };
    void tick();
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      stopped = true;
      clearTimeout(timer);
      controller.abort();
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [intervalMs, enabled]);
}
