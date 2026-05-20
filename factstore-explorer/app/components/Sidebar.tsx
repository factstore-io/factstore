import { NavLink } from "react-router";
import type { Store } from "~/types";

interface Props {
  currentStore: Store | undefined;
}

export default function Sidebar({ currentStore }: Props) {
  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <span className="sidebar-brand-name">FactStore Explorer</span>
        <span className="sidebar-brand-sub">Event Store UI</span>
      </div>

      <div className="sidebar-section" style={{ paddingTop: "var(--gap-md)" }}>
        <span className="sidebar-label">Navigation</span>
        <nav className="sidebar-nav">
          <NavLink to="/" end>
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.7"
            >
              <rect x="3" y="3" width="18" height="18" rx="2" />
              <path d="M3 9h18M9 3v18" strokeLinecap="round" />
            </svg>
            Stores
          </NavLink>
        </nav>
      </div>

      {currentStore && (
        <div
          className="sidebar-section"
          style={{ paddingTop: "var(--gap-md)" }}
        >
          <span className="sidebar-label">Current Store</span>
          <div className="sidebar-store-ctx">
            <span className="sidebar-store-ctx-name">{currentStore.name}</span>
            <span>scope selected</span>
          </div>
          <nav className="sidebar-nav">
            <NavLink to={`/stores/${currentStore.id}/facts`}>
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.7"
              >
                <path
                  d="M4 6h16M4 10h16M4 14h10M4 18h6"
                  strokeLinecap="round"
                />
              </svg>
              Fact Browser
            </NavLink>
            <NavLink to={`/stores/${currentStore.id}/stream`}>
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.7"
              >
                <circle cx="12" cy="12" r="3" />
                <path
                  d="M6.5 6.5a8 8 0 0 0 0 11M17.5 6.5a8 8 0 0 1 0 11"
                  strokeLinecap="round"
                />
                <path
                  d="M4 4a12 12 0 0 0 0 16M20 4a12 12 0 0 1 0 16"
                  strokeLinecap="round"
                />
              </svg>
              Live Stream
            </NavLink>
          </nav>
        </div>
      )}

      <div className="sidebar-bottom">
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 11,
            color: "var(--muted)",
          }}
        >
          factstore v0.1.0-beta
        </span>
      </div>
    </aside>
  );
}
