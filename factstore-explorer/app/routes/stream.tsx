import { useState, useRef, useEffect } from "react";
import { useParams, useOutletContext } from "react-router";
import type { AppShellContext } from "~/layouts/AppShell";
import type { StreamEntry } from "~/types";
import { getStoreTypes, getStoreSubjects } from "~/lib/mockData";
import { fmtN, getTypeClass } from "~/lib/utils";

export function meta() {
  return [{ title: "Live Stream — FactStore Explorer" }];
}

type StreamMode = "tail" | "replay" | "fromid";

export default function StreamRoute() {
  const { storeId } = useParams<{ storeId: string }>();
  const { stores, currentStore } = useOutletContext<AppShellContext>();

  const types = storeId ? getStoreTypes(storeId, stores) : [];
  const subjects = storeId ? getStoreSubjects(storeId, stores) : [];

  const [mode, setMode] = useState<StreamMode>("tail");
  const [fromId, setFromId] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [frozen, setFrozen] = useState(false);
  const [entries, setEntries] = useState<StreamEntry[]>([]);
  const [bufferCount, setBufferCount] = useState(0);
  const [total, setTotal] = useState(0);
  const [rate, setRate] = useState("0.0");

  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const logRef = useRef<HTMLDivElement>(null);
  const frozenRef = useRef(false);
  const bufferRef = useRef<StreamEntry[]>([]);
  const rateTsRef = useRef<number[]>([]);

  useEffect(() => {
    frozenRef.current = frozen;
  }, [frozen]);

  function makeEntry(): StreamEntry {
    return {
      id: crypto.randomUUID(),
      ts: new Date().toISOString().slice(11, 23),
      type: types[Math.floor(Math.random() * types.length)],
      subject: subjects[Math.floor(Math.random() * subjects.length)],
      factId: Math.random().toString(36).slice(2, 10),
      isNew: true,
    };
  }

  function startStream() {
    if (mode === "replay") {
      setEntries([]);
      setTotal(0);
    }
    frozenRef.current = false;
    bufferRef.current = [];
    setFrozen(false);
    setBufferCount(0);
    rateTsRef.current = [];
    setStreaming(true);

    timerRef.current = setInterval(() => {
      const entry = makeEntry();
      const now = Date.now();
      rateTsRef.current = [
        ...rateTsRef.current.filter((t) => now - t < 5000),
        now,
      ];
      setRate((rateTsRef.current.length / 5).toFixed(1));
      setTotal((t) => t + 1);

      if (frozenRef.current) {
        bufferRef.current = [...bufferRef.current, entry];
        setBufferCount(bufferRef.current.length);
      } else {
        setEntries((prev) => [...prev, entry]);
      }
    }, 440);
  }

  function stopStream() {
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = null;
    setStreaming(false);
  }

  function freeze() {
    frozenRef.current = true;
    setFrozen(true);
  }

  function resumeStream() {
    const toFlush = [...bufferRef.current];
    bufferRef.current = [];
    frozenRef.current = false;
    setFrozen(false);
    setBufferCount(0);
    setEntries((prev) => [...prev, ...toFlush]);
  }

  function toggleFreeze() {
    if (!streaming) return;
    if (frozenRef.current) resumeStream();
    else freeze();
  }

  useEffect(() => {
    if (!frozen && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [entries, frozen]);

  useEffect(() => {
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, []);

  const dotClass = frozen ? "live-dot paused" : streaming ? "live-dot live" : "live-dot";
  const statusText = frozen ? "Frozen" : streaming ? "Live" : "Disconnected";

  return (
    <>
      <div
        className="row-between view-header"
        style={{ marginBottom: "var(--gap-lg)" }}
      >
        <div>
          <p className="eyebrow">Live Stream</p>
          <h1>Stream{currentStore ? ` · ${currentStore.name}` : ""}</h1>
          <p className="view-sub">
            Real-time fact ingestion — tail, replay, or seek to a fact ID.
          </p>
        </div>
      </div>

      <div className="stream-shell">
        <div className="stream-controls">
          <div className="pill-group">
            {(["tail", "replay", "fromid"] as const).map((m) => (
              <button
                key={m}
                className={`pill-btn${mode === m ? " active" : ""}`}
                onClick={() => setMode(m)}
              >
                {m === "tail"
                  ? "Tail (latest)"
                  : m === "replay"
                    ? "Replay all"
                    : "From ID"}
              </button>
            ))}
          </div>

          {mode === "fromid" && (
            <input
              className="filter-input"
              placeholder="Fact UUID to start from…"
              style={{ width: 260, padding: "5px 10px" }}
              value={fromId}
              onChange={(e) => setFromId(e.target.value)}
            />
          )}

          <div
            style={{ marginLeft: "auto", display: "flex", gap: "var(--gap-sm)" }}
          >
            <button
              className="btn btn-ghost btn-sm"
              style={frozen ? { color: "var(--warn)" } : {}}
              onClick={toggleFreeze}
            >
              <svg
                width="12"
                height="12"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
              >
                <path d="M12 2v20M2 12h20" strokeLinecap="round" />
                <path d="M5 5l14 14M19 5 5 19" strokeLinecap="round" />
              </svg>
              Freeze
            </button>
            <button
              className={`btn btn-sm ${streaming ? "btn-danger" : "btn-success"}`}
              onClick={streaming ? stopStream : startStream}
            >
              {streaming ? (
                <>
                  <svg
                    width="11"
                    height="11"
                    viewBox="0 0 24 24"
                    fill="currentColor"
                  >
                    <rect x="6" y="5" width="4" height="14" rx="1" />
                    <rect x="14" y="5" width="4" height="14" rx="1" />
                  </svg>
                  Disconnect
                </>
              ) : (
                <>
                  <svg
                    width="11"
                    height="11"
                    viewBox="0 0 24 24"
                    fill="currentColor"
                  >
                    <polygon points="5,3 19,12 5,21" />
                  </svg>
                  Connect
                </>
              )}
            </button>
          </div>
        </div>

        <div className="stream-stats">
          <span className={dotClass} />
          <span>{statusText}</span>
          <span style={{ marginLeft: "auto" }} className="num">
            {fmtN(total)} events
          </span>
          <span className="num">{rate} / sec</span>
        </div>

        <div className="stream-log" ref={logRef}>
          <div
            className={`stream-freeze-banner${frozen ? " visible" : ""}`}
          >
            <span>
              ⏸ Stream frozen — {bufferCount} events buffered
            </span>
            <button className="btn btn-ghost btn-sm" onClick={resumeStream}>
              Resume
            </button>
          </div>
          <div className="stream-log-inner">
            {entries.length === 0 ? (
              <div className="stream-empty">
                <svg
                  width="28"
                  height="28"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.3"
                  style={{ color: "var(--border)" }}
                >
                  <circle cx="12" cy="12" r="3" />
                  <circle cx="12" cy="12" r="7" strokeDasharray="2 3" />
                  <circle cx="12" cy="12" r="11" strokeDasharray="2 4" />
                </svg>
                <span>Connect to start receiving facts</span>
              </div>
            ) : (
              entries.map((entry) => (
                <div
                  key={entry.id}
                  className={`stream-entry${entry.isNew ? " new" : ""}`}
                >
                  <span className="stream-ts">{entry.ts}</span>
                  <span className={`stream-ftype ${getTypeClass(entry.type)}`}>
                    {entry.type}
                  </span>
                  <span className="stream-subj">{entry.subject}</span>
                  <span className="stream-fid">{entry.factId}</span>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </>
  );
}
