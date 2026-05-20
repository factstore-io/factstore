import { useEffect } from "react";
import type { Store } from "~/types";
import { fmtN } from "~/lib/utils";

interface Props {
  open: boolean;
  store: Store | null;
  onClose: () => void;
  onDelete: () => void;
}

export default function DeleteStoreModal({
  open,
  store,
  onClose,
  onDelete,
}: Props) {
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  function handleOverlayClick(e: React.MouseEvent) {
    if (e.target === e.currentTarget) onClose();
  }

  const message = store
    ? `Permanently delete "${store.name}" and all ${fmtN(store.factCount)} facts?`
    : "This will permanently remove all facts.";

  return (
    <div
      className={`modal-overlay${open ? " open" : ""}`}
      onClick={handleOverlayClick}
    >
      <div className="confirm-box">
        <h3>Delete store?</h3>
        <p>{message}</p>
        <div className="confirm-actions">
          <button className="btn btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-danger" onClick={onDelete}>
            Delete
          </button>
        </div>
      </div>
    </div>
  );
}
