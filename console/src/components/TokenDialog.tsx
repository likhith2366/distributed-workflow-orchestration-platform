import { useState, type FormEvent } from "react";
import { Modal } from "./common";

export function TokenDialog({ token, onSave, onClose }: { token: string; onSave: (t: string) => void; onClose: () => void }) {
  const [value, setValue] = useState(token);
  const save = (event: FormEvent) => {
    event.preventDefault();
    onSave(value.trim());
  };
  return (
    <Modal title="API token" onClose={onClose}>
      <form onSubmit={save}>
        <p className="muted">Stored for this browser tab only (sessionStorage). Required when the coordinator sets API_TOKEN.</p>
        <label htmlFor="token">BEARER TOKEN</label>
        <input id="token" type="password" autoComplete="off" value={value} onChange={(e) => setValue(e.target.value)} />
        <button type="submit">Save token</button>
      </form>
    </Modal>
  );
}
