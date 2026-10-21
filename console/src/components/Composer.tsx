import { useState, type FormEvent } from "react";
import { PRESETS } from "../presets";
import type { WorkflowDefinition } from "../types";
import { Modal } from "./common";

interface Props {
  onClose: () => void;
  onSubmit: (definition: WorkflowDefinition) => Promise<void>;
}

/** Light structural checks; the server performs full DAG validation and returns precise errors. */
export function parseDefinition(text: string): WorkflowDefinition {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (e) {
    throw new Error(`Invalid JSON: ${(e as Error).message}`);
  }
  const d = value as Partial<WorkflowDefinition> | null;
  if (!d || typeof d !== "object" || Array.isArray(d)) throw new Error("The definition must be a JSON object");
  if (typeof d.name !== "string" || !d.name) throw new Error("name is required");
  if (typeof d.version !== "string" || !d.version) throw new Error("version is required (as a string)");
  if (!Array.isArray(d.steps) || d.steps.length === 0) throw new Error("steps must be a non-empty array");
  return d as WorkflowDefinition;
}

export function Composer({ onClose, onSubmit }: Props) {
  const [text, setText] = useState(() => JSON.stringify(PRESETS[0]!.definition, null, 2));
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      await onSubmit(parseDefinition(text));
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  };
  return (
    <Modal title="Start an execution" onClose={onClose}>
      <form onSubmit={submit}>
        <p className="muted">Choose a scenario, or edit the workflow definition. It is pinned to the execution at start.</p>
        <div className="presets">
          {PRESETS.map((p) => (
            <button
              key={p.id}
              type="button"
              className="secondary preset"
              title={p.description}
              onClick={() => {
                setText(JSON.stringify(p.definition, null, 2));
                setError("");
              }}
            >
              {p.label}
            </button>
          ))}
        </div>
        <label htmlFor="definition">WORKFLOW JSON</label>
        <textarea id="definition" spellCheck={false} required value={text} onChange={(e) => setText(e.target.value)} />
        <p className="form-error" role="alert">
          {error}
        </p>
        <button type="submit" disabled={submitting}>
          {submitting ? "Starting…" : "Start workflow →"}
        </button>
      </form>
    </Modal>
  );
}
