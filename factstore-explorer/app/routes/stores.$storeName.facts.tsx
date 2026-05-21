import { useState, useCallback } from "react"
import { useParams } from "react-router"
import { Search, X, Plus, AlertCircle } from "lucide-react"
import { Button } from "~/components/ui/button"
import { Input } from "~/components/ui/input"
import { Label } from "~/components/ui/label"
import { Alert, AlertDescription } from "~/components/ui/alert"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select"
import { FactTable } from "~/components/FactTable"
import { queryFacts, type Fact } from "~/lib/api"

export function meta({ params }: { params: { storeName: string } }) {
  return [{ title: `Facts — ${params.storeName} — FactStore Explorer` }]
}

type QueryMode = "timeRange" | "tags" | "subject"

type TimePreset = "5m" | "15m" | "1h" | "6h" | "24h" | "custom"

function resolvePreset(preset: TimePreset): { from: string; to: string } | null {
  if (preset === "custom") return null
  const now = new Date()
  const ms: Record<string, number> = {
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "24h": 24 * 60 * 60_000,
  }
  return {
    from: new Date(now.getTime() - ms[preset]).toISOString(),
    to: now.toISOString(),
  }
}

export default function FactsPage() {
  const { storeName } = useParams<{ storeName: string }>()

  const [mode, setMode] = useState<QueryMode>("timeRange")

  // time range
  const [preset, setPreset] = useState<TimePreset>("1h")
  const [customFrom, setCustomFrom] = useState("")
  const [customTo, setCustomTo] = useState("")

  // tags
  const [tagInputs, setTagInputs] = useState<{ key: string; value: string }[]>([{ key: "", value: "" }])

  // subject
  const [subject, setSubject] = useState("")

  // query options
  const [limit, setLimit] = useState("100")
  const [direction, setDirection] = useState<"forward" | "backward">("backward")

  // results
  const [facts, setFacts] = useState<Fact[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [queried, setQueried] = useState(false)

  const runQuery = useCallback(async () => {
    if (!storeName) return
    setLoading(true)
    setError(null)
    try {
      let result: Fact[]
      if (mode === "timeRange") {
        const range = preset !== "custom" ? resolvePreset(preset) : null
        result = await queryFacts(storeName, {
          mode: "timeRange",
          from: range?.from ?? (customFrom ? new Date(customFrom).toISOString() : undefined),
          to: range?.to ?? (customTo ? new Date(customTo).toISOString() : undefined),
          limit: Number(limit) || 0,
          direction,
        })
      } else if (mode === "tags") {
        const cleanTags = tagInputs
          .filter((t) => t.key.trim() && t.value.trim())
          .map((t) => `${t.key.trim()}=${t.value.trim()}`)
        if (cleanTags.length === 0) {
          setError("Add at least one complete tag filter (key and value) before running a tag query.")
          setLoading(false)
          return
        }
        result = await queryFacts(storeName, {
          mode: "tags",
          tags: cleanTags,
          limit: Number(limit) || 0,
          direction,
        })
      } else {
        result = await queryFacts(storeName, {
          mode: "subject",
          subject: subject.trim(),
          limit: Number(limit) || 0,
          direction,
        })
      }
      setFacts(result)
      setQueried(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed")
    } finally {
      setLoading(false)
    }
  }, [storeName, mode, preset, customFrom, customTo, tagInputs, subject, limit, direction])

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      {/* Filter panel */}
      <div className="rounded-xl border border-border bg-card p-5 space-y-4">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <h2 className="font-semibold text-sm">Query</h2>
          <div className="flex items-center gap-2 flex-wrap">
            {/* Mode selector */}
            <div className="flex rounded-lg border border-border overflow-hidden text-xs">
              {(["timeRange", "tags", "subject"] as QueryMode[]).map((m) => (
                <button
                  key={m}
                  className={`px-3 py-1.5 font-medium transition-colors ${
                    mode === m
                      ? "bg-foreground text-background"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                  onClick={() => setMode(m)}
                >
                  {m === "timeRange" ? "Time Range" : m === "tags" ? "Tags" : "Subject"}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Time range controls */}
        {mode === "timeRange" && (
          <div className="space-y-3">
            <div className="flex flex-wrap gap-1.5">
              {(["5m", "15m", "1h", "6h", "24h", "custom"] as TimePreset[]).map((p) => (
                <button
                  key={p}
                  onClick={() => setPreset(p)}
                  className={`rounded-md border px-3 py-1 text-xs font-medium transition-colors ${
                    preset === p
                      ? "border-foreground bg-foreground text-background"
                      : "border-border text-muted-foreground hover:border-foreground hover:text-foreground"
                  }`}
                >
                  {p === "custom"
                    ? "Custom"
                    : p === "5m"
                    ? "Last 5 min"
                    : p === "15m"
                    ? "Last 15 min"
                    : p === "1h"
                    ? "Last 1h"
                    : p === "6h"
                    ? "Last 6h"
                    : "Last 24h"}
                </button>
              ))}
            </div>
            {preset === "custom" && (
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-1">
                  <Label className="text-xs">From</Label>
                  <Input
                    type="datetime-local"
                    value={customFrom}
                    onChange={(e) => setCustomFrom(e.target.value)}
                    className="text-xs h-8"
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">To</Label>
                  <Input
                    type="datetime-local"
                    value={customTo}
                    onChange={(e) => setCustomTo(e.target.value)}
                    className="text-xs h-8"
                  />
                </div>
              </div>
            )}
          </div>
        )}

        {/* Tags controls */}
        {mode === "tags" && (
          <div className="space-y-2">
            {tagInputs.length > 0 && (
              <div className="grid grid-cols-[1fr_auto_1fr_auto] items-center gap-2 mb-1">
                <Label className="text-xs">Key</Label>
                <span />
                <Label className="text-xs">Value</Label>
                <span />
              </div>
            )}
            {tagInputs.map((tag, i) => (
              <div key={i} className="grid grid-cols-[1fr_auto_1fr_auto] items-center gap-2">
                <Input
                  value={tag.key}
                  onChange={(e) => {
                    const next = [...tagInputs]
                    next[i] = { ...next[i], key: e.target.value }
                    setTagInputs(next)
                  }}
                  placeholder="e.g. env"
                  className="font-mono text-xs h-8"
                />
                <span className="text-muted-foreground text-xs select-none">=</span>
                <Input
                  value={tag.value}
                  onChange={(e) => {
                    const next = [...tagInputs]
                    next[i] = { ...next[i], value: e.target.value }
                    setTagInputs(next)
                  }}
                  placeholder="e.g. production"
                  className="font-mono text-xs h-8"
                />
                <Button
                  variant="ghost"
                  size="icon-sm"
                  disabled={tagInputs.length === 1}
                  onClick={() => setTagInputs(tagInputs.filter((_, j) => j !== i))}
                >
                  <X className="size-3.5" />
                </Button>
              </div>
            ))}
            <Button
              variant="ghost"
              size="sm"
              className="text-xs h-7"
              onClick={() => setTagInputs([...tagInputs, { key: "", value: "" }])}
            >
              <Plus className="size-3" />
              Add tag filter
            </Button>
          </div>
        )}

        {/* Subject controls */}
        {mode === "subject" && (
          <div className="space-y-1.5">
            <Label className="text-xs">Subject</Label>
            <Input
              value={subject}
              onChange={(e) => setSubject(e.target.value)}
              placeholder="e.g. user:123, order:456"
              className="font-mono text-xs h-8 max-w-sm"
            />
          </div>
        )}

        {/* Query options + run */}
        <div className="flex flex-wrap items-end gap-3 pt-1 border-t border-border">
          <div className="space-y-1">
            <Label className="text-xs">Limit</Label>
            <Select value={limit} onValueChange={setLimit}>
              <SelectTrigger className="h-8 w-24 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="50">50</SelectItem>
                <SelectItem value="100">100</SelectItem>
                <SelectItem value="250">250</SelectItem>
                <SelectItem value="500">500</SelectItem>
                <SelectItem value="0">Unlimited</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-1">
            <Label className="text-xs">Order</Label>
            <Select value={direction} onValueChange={(v) => setDirection(v as "forward" | "backward")}>
              <SelectTrigger className="h-8 w-32 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="backward">Newest first</SelectItem>
                <SelectItem value="forward">Oldest first</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <Button onClick={runQuery} disabled={loading} className="ml-auto">
            <Search className="size-3.5" />
            {loading ? "Querying…" : "Run Query"}
          </Button>
        </div>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="size-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Results */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        {queried && (
          <div className="flex items-center justify-between gap-4 px-5 py-3 border-b border-border">
            <span className="text-xs font-mono text-muted-foreground">
              {facts.length} fact{facts.length !== 1 ? "s" : ""}
            </span>
          </div>
        )}
        <FactTable
          facts={facts}
          loading={loading}
          emptyMessage={queried ? "No facts match your query." : "Run a query to see results."}
        />
      </div>
    </div>
  )
}
