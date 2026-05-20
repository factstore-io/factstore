import { useState } from "react";
import { Link, useNavigate, useOutletContext } from "react-router";
import type { AppShellContext } from "~/layouts/AppShell";
import type { Store } from "~/types";
import CreateStoreModal from "~/components/CreateStoreModal";
import DeleteStoreModal from "~/components/DeleteStoreModal";
import { fmtDate, fmtN, trunc } from "~/lib/utils";

export function meta() {
  return [{ title: "Stores — FactStore Explorer" }];
}

export default function StoresRoute() {
  const { stores, setStores } = useOutletContext<AppShellContext>();
  const navigate = useNavigate();

  const [createOpen, setCreateOpen] = useState(false);
  const [deleteStore, setDeleteStore] = useState<Store | null>(null);

  function handleCreate(store: Store) {
    setStores((prev) => [...prev, store]);
  }

  function handleDelete() {
    if (!deleteStore) return;
    setStores((prev) => prev.filter((s) => s.id !== deleteStore.id));
    setDeleteStore(null);
  }

  return (
    <>
      <div className="row-between view-header">
        <div>
          <p className="eyebrow">Event Store</p>
          <h1>Stores</h1>
          <p className="view-sub">
            Logical isolation boundaries for your fact streams.
          </p>
        </div>
        <button className="btn btn-success" onClick={() => setCreateOpen(true)}>
          <svg
            width="13"
            height="13"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.5"
          >
            <path d="M12 5v14M5 12h14" strokeLinecap="round" />
          </svg>
          New Store
        </button>
      </div>

      {stores.length === 0 ? (
        <div className="empty-state">
          <h3>No stores</h3>
          <p>Create your first store to begin appending and querying facts.</p>
          <button
            className="btn btn-success"
            onClick={() => setCreateOpen(true)}
          >
            Create Store
          </button>
        </div>
      ) : (
        <div className="card" style={{ padding: 0, overflow: "hidden" }}>
          <table className="ds-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Store ID</th>
                <th>Created</th>
                <th style={{ textAlign: "right" }}>Facts</th>
                <th style={{ textAlign: "right" }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {stores.map((store) => (
                <tr key={store.id}>
                  <td>
                    <Link
                      to={`/stores/${store.id}/facts`}
                      style={{
                        fontWeight: 600,
                        fontSize: 15,
                        letterSpacing: "-0.01em",
                        color: "var(--fg)",
                      }}
                    >
                      {store.name}
                    </Link>
                  </td>
                  <td className="truncated">{trunc(store.id)}</td>
                  <td
                    className="num"
                    style={{ color: "var(--muted)", fontSize: 12 }}
                  >
                    {fmtDate(store.created)}
                  </td>
                  <td className="num" style={{ textAlign: "right" }}>
                    {fmtN(store.factCount)}
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <span
                      className="row"
                      style={{ justifyContent: "flex-end", gap: 4 }}
                    >
                      <button
                        className="btn btn-ghost btn-sm"
                        onClick={() =>
                          navigate(`/stores/${store.id}/facts`)
                        }
                      >
                        Browse
                      </button>
                      <button
                        className="btn btn-ghost btn-sm"
                        onClick={() =>
                          navigate(`/stores/${store.id}/stream`)
                        }
                      >
                        Stream
                      </button>
                      <button
                        className="btn btn-danger btn-sm"
                        onClick={() => setDeleteStore(store)}
                      >
                        Delete
                      </button>
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <CreateStoreModal
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreate={handleCreate}
      />
      <DeleteStoreModal
        open={deleteStore !== null}
        store={deleteStore}
        onClose={() => setDeleteStore(null)}
        onDelete={handleDelete}
      />
    </>
  );
}
