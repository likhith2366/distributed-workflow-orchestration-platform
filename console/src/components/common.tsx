import { useEffect, useRef, type ReactNode } from "react";

export function StatusBadge({ status }: { status: string }) {
  return <span className={`status status-${status.toLowerCase()}`}>{status.replace("_", " ")}</span>;
}

export function relativeTime(at: number, now: number): string {
  const seconds = Math.round((now - at) / 1000);
  if (Math.abs(seconds) < 5) return "just now";
  const future = seconds < 0;
  const s = Math.abs(seconds);
  const text = s < 60 ? `${s}s` : s < 3600 ? `${Math.floor(s / 60)}m` : s < 86400 ? `${Math.floor(s / 3600)}h` : `${Math.floor(s / 86400)}d`;
  return future ? `in ${text}` : `${text} ago`;
}

/** Minimal accessible modal: focuses its first field, closes on Escape and backdrop click. */
export function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  const panel = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    panel.current?.querySelector<HTMLElement>("textarea, input, button:not(.close)")?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      previous?.focus();
    };
  }, [onClose]);
  return (
    <div className="backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="modal" role="dialog" aria-modal="true" aria-label={title} ref={panel}>
        <div className="modal-title">
          <h2>{title}</h2>
          <button type="button" className="secondary close" onClick={onClose}>
            Close
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}
