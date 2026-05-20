import { useState, useMemo } from "react";
import { useParams, useNavigate, useOutletContext } from "react-router";
import type { AppShellContext } from "~/layouts/AppShell";
import type { Fact, Tag } from "~/types";
import { generateFacts } from "~/lib/mockData";
import { fmtDate, trunc, getTypeClass } from "~/lib/utils";

export function meta() {
  return [{ title: "Fact Browser — FactStore Explorer" }];
}

function TagChips({ tags }: { tags: Tag[] }) {
  return (
    <>
      {tags.map((t, i) => (
        <span className="tag" key={i}>
          {t.k}
          <span className="tag-sep">:</span>
          <span className="tag-val">{t.v}</span>
        </span>
      ))}
    </>
  );
}

function FactRow({ fact }: { fact: Fact }) {
  const [expanded, setExpanded] = useState(false);
  const tc = getTypeClass(fact.factType);

  return (
    <>
      <tr className="fact-row" onClick={() => setExpanded((e) => !e)}>
        <td className="truncated">{trunc(fact.id)}</td>
        <td
          className="num"
          style={{ whiteSpace: "nowrap", color: "var(--muted)", fontSize: 12 }}
        >
          {fmtDate(fact.appendedAt)}
        </td>
        <td>
          <span className={`fact-type ${tc}`}>{fact.factType}</span>
        </td>
        <td style={{ fontSize: 13 }}>{fact.subject}</td>
        <td>
          <TagChips tags={fact.tags} />
        </td>
        <td
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 10,
            width: 20,
            textAlign: "right",
            color: expanded ? "var(--accent)" : "var(--muted)",
            transition: "color 0.1s",
          }}
        >
          {expanded ? "▾" : "▸"}
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={6}>
            <div className="fact-detail-inner">
              <div className="detail-grid">
                <span className="detail-label">Fact ID</span>
                <code
                  style={{
                    fontSize: 12,
                    color: "var(--fg)",
                    wordBreak: "break-all",
                  }}
                >
                  {fact.id}
                </code>

                <span className="detail-label">Appended At</span>
                <span className="num" style={{ fontSize: 12 }}>
                  {fmtDate(fact.appendedAt)}
                </span>

                <span className="detail-label">Tags</span>
                <span>
                  <TagChips tags={fact.tags} />
                </span>

                {fact.metadata.length > 0 && (
                  <>
                    <span className="detail-label">Metadata</span>
                    <span>
                      <TagChips tags={fact.metadata} />
                    </span>
                  </>
                )}

                <span className="detail-label">Payload</span>
                <div>
                  <pre className="payload-block">{fact.payload}</pre>
                </div>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export default function FactsRoute() {
  const { storeId } = useParams<{ storeId: string }>();
  const { stores, currentStore } = useOutletContext<AppShellContext>();
  const navigate = useNavigate();

  const allFacts = useMemo(
    () => (storeId ? generateFacts(storeId, stores) : []),
    [storeId, stores]
  );

  const [tagKey, setTagKey] = useState("");
  const [tagVal, setTagVal] = useState("");
  const [subject, setSubject] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [applied, setApplied] = useState({
    tagKey: "",
    tagVal: "",
    subject: "",
    from: "",
    to: "",
  });

  const shownFacts = useMemo(() => {
    const { tagKey: tk, tagVal: tv, subject: sb, from: fr, to: t } = applied;
    return allFacts.filter((f) => {
      if (sb && !f.subject.toLowerCase().includes(sb.toLowerCase()))
        return false;
      if (
        (tk || tv) &&
        !f.tags.some(
          (tag) =>
            (!tk || tag.k.toLowerCase().includes(tk.toLowerCase())) &&
            (!tv || tag.v.toLowerCase().includes(tv.toLowerCase()))
        )
      )
        return false;
      if (fr && new Date(f.appendedAt) < new Date(fr)) return false;
      if (t && new Date(f.appendedAt) > new Date(t)) return false;
      return true;
    });
  }, [allFacts, applied]);

  function applyFilters() {
    setApplied({ tagKey, tagVal, subject, from, to });
  }

  function clearFilters() {
    setTagKey("");
    setTagVal("");
    setSubject("");
    setFrom("");
    setTo("");
    setApplied({ tagKey: "", tagVal: "", subject: "", from: "", to: "" });
  }

  return (
    <>
      <div className="row-between view-header">
        <div>
          <p className="eyebrow">Fact Browser</p>
          <h1>Facts{currentStore ? ` in ${currentStore.name}` : ""}</h1>
          <p className="view-sub">
            Query facts by tag, subject, or time range.
          </p>
        </div>
        {storeId && (
          <button
            className="btn btn-ghost"
            onClick={() => navigate(`/stores/${storeId}/stream`)}
          >
            <svg
              width="13"
              height="13"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.8"
            >
              <circle cx="12" cy="12" r="3" />
              <circle cx="12" cy="12" r="7" strokeDasharray="3 3" />
            </svg>
            Live Stream →
          </button>
        )}
      </div>

      <div className="filter-bar">
        <div className="filter-field">
          <span className="filter-label">Tag Key</span>
          <input
            className="filter-input"
            placeholder="e.g. customerId"
            value={tagKey}
            onChange={(e) => setTagKey(e.target.value)}
          />
        </div>
        <div className="filter-field">
          <span className="filter-label">Tag Value</span>
          <input
            className="filter-input"
            placeholder="cust-441"
            value={tagVal}
            onChange={(e) => setTagVal(e.target.value)}
          />
        </div>
        <div className="filter-field">
          <span className="filter-label">Subject</span>
          <input
            className="filter-input"
            placeholder="order/ORD-…"
            value={subject}
            onChange={(e) => setSubject(e.target.value)}
          />
        </div>
        <div className="filter-field">
          <span className="filter-label">From</span>
          <input
            className="filter-input"
            type="datetime-local"
            style={{ colorScheme: "dark", maxWidth: 190 }}
            value={from}
            onChange={(e) => setFrom(e.target.value)}
          />
        </div>
        <div className="filter-field">
          <span className="filter-label">To</span>
          <input
            className="filter-input"
            type="datetime-local"
            style={{ colorScheme: "dark", maxWidth: 190 }}
            value={to}
            onChange={(e) => setTo(e.target.value)}
          />
        </div>
        <div
          style={{
            alignSelf: "flex-end",
            display: "flex",
            gap: "var(--gap-sm)",
          }}
        >
          <button className="btn btn-secondary btn-sm" onClick={clearFilters}>
            Clear
          </button>
          <button className="btn btn-primary btn-sm" onClick={applyFilters}>
            Apply
          </button>
        </div>
      </div>

      <div className="table-meta">
        <span>
          Showing <strong>{shownFacts.length}</strong> of {allFacts.length}{" "}
          facts
        </span>
        <span className="badge">{shownFacts.length} rows</span>
      </div>
      <div className="card" style={{ padding: 0, overflow: "hidden" }}>
        <table className="ds-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Appended At</th>
              <th>Fact Type</th>
              <th>Subject</th>
              <th>Tags</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {shownFacts.map((fact) => (
              <FactRow key={fact.id} fact={fact} />
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
