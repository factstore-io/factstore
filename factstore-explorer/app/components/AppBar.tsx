import { Link, useLocation } from "react-router";
import type { Store } from "~/types";

interface Props {
  currentStore: Store | undefined;
}

export default function AppBar({ currentStore }: Props) {
  const location = useLocation();
  const isStream = location.pathname.endsWith("/stream");

  return (
    <header className="appbar">
      <div className="breadcrumb">
        {!currentStore ? (
          <span className="current">Stores</span>
        ) : (
          <>
            <Link to="/" style={{ color: "var(--muted)" }}>
              Stores
            </Link>
            <span className="sep">/</span>
            <Link to="/" style={{ color: "var(--muted)" }}>
              {currentStore.name}
            </Link>
            <span className="sep">/</span>
            <span className="current">
              {isStream ? "Live Stream" : "Fact Browser"}
            </span>
          </>
        )}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: "var(--gap-sm)" }}>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 11,
            color: "#4ade80",
          }}
        >
          ● connected
        </span>
      </div>
    </header>
  );
}
