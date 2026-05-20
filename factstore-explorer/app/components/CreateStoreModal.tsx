import { useEffect, useRef } from "react";
import type { Store } from "~/types";

interface Props {
  open: boolean;
  onClose: () => void;
  onCreate: (store: Store) => void;
}

export default function CreateStoreModal({ open, onClose, onCreate }: Props) {
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 60);
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  function handleCreate() {
    const raw = inputRef.current?.value.trim() ?? "";
    if (!raw) return;
    const name = raw.toLowerCase().replace(/[^a-z0-9-]/g, "-");
    onCreate({
      id: `new0${Date.now().toString(16)}-0000-0000-0000-000000000000`,
      name,
      created: new Date().toISOString(),
      factCount: 0,
    });
    if (inputRef.current) inputRef.current.value = "";
    onClose();
  }

  function handleOverlayClick(e: React.MouseEvent) {
    if (e.target === e.currentTarget) onClose();
  }

  return (
    <div
      className={`modal-overlay${open ? " open" : ""}`}
      onClick={handleOverlayClick}
    >
      <div className="modal">
        <h2>New Store</h2>
        <div className="field">
          <label htmlFor="store-name-input">Store Name</label>
          <input
            ref={inputRef}
            className="input"
            id="store-name-input"
            type="text"
            placeholder="e.g. orders, payments, audit-log"
            autoComplete="off"
            onKeyDown={(e) => e.key === "Enter" && handleCreate()}
          />
          <span style={{ fontSize: 12, color: "var(--muted)" }}>
            Lowercase, numbers, and hyphens only.
          </span>
        </div>
        <div className="modal-actions">
          <button className="btn btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-success" onClick={handleCreate}>
            Create Store
          </button>
        </div>
      </div>
    </div>
  );
}
