import { useState, useCallback, useRef } from "react"
import { useParams } from "react-router"
import { Search, X, Plus, AlertCircle } from "lucide-react"
import type { Route } from "./+types/stores.$storeName.facts"
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
import { queryFacts, type Fact, type FactFilter } from "~/lib/api"

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `Facts — ${params.storeName} — FactStore Explorer` }]
}

type QueryMode = "all" | "tags" | "subject" | "type" | "query"


type QueryFilterInput = { id: number; subjects: string; types: string; tags: string }

const MODE_LABELS: Record<QueryMode, string> = {
  all: "All Facts",
  tags: "Tags",
  subject: "Subject",
  type: "Type",
  query: "Query",
}

/** Turns the comma-separated inputs of one filter into the shape the API expects. */
function toFactFilter(input: QueryFilterInput): FactFilter {
  const values = (raw: string) => raw.split(",").map((value) => value.trim()).filter(Boolean)
  const filter: FactFilter = {}

  const subjects = values(input.subjects)
  if (subjects.length > 0) filter.subjects = subjects

  const types = values(input.types)
  if (types.length > 0) filter.types = types

  const tags = values(input.tags)
    .map((tag) => tag.split("=", 2))
    .filter((parts) => parts.length === 2 && parts[0] && parts[1])
  if (tags.length > 0) filter.tags = Object.fromEntries(tags)

  return filter
}

export default function FactsPage() {
  const { storeName } = useParams<{ storeName: string }>()

  const [mode, setMode] = useState<QueryMode>("all")

  // tags
  const tagIdRef = useRef(1)
  const [tagInputs, setTagInputs] = useState<{ id: number; key: string; value: string }[]>([{ id: 0, key: "", value: "" }])

  // subject
  const [subject, setSubject] = useState("")

  // type
  const [type, setType] = useState("")

  // query: each filter matches subjects, types and tags; a fact matches when any filter does
  const filterIdRef = useRef(1)
  const [queryFilters, setQueryFilters] = useState<QueryFilterInput[]>([
    { id: 0, subjects: "", types: "", tags: "" },
  ])

  // query options
  const [limit, setLimit] = useState("100")
  const [direction, setDirection] = useState<"forward" | "backward">("backward")

  // results
  const [facts, setFacts] = useState<Fact[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [queried, setQueried] = useState(false)

  const updateFilter = (index: number, patch: Partial<QueryFilterInput>) =>
    setQueryFilters((filters) => filters.map((filter, i) => (i === index ? { ...filter, ...patch } : filter)))

  const runQuery = useCallback(async () => {
    if (!storeName) return
    setLoading(true)
    setError(null)
    try {
      let result: Fact[]
      if (mode === "all") {
        result = await queryFacts(storeName, {
          mode: "all",
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
      } else if (mode === "subject") {
        result = await queryFacts(storeName, {
          mode: "subject",
          subject: subject.trim(),
          limit: Number(limit) || 0,
          direction,
        })
      } else if (mode === "type") {
        result = await queryFacts(storeName, {
          mode: "type",
          type: type.trim(),
          limit: Number(limit) || 0,
          direction,
        })
      } else {
        const filters = queryFilters.map(toFactFilter).filter((filter) => Object.keys(filter).length > 0)
        if (filters.length === 0) {
          setError("Add at least one filter with a subject, a type or a tag before running a query.")
          setLoading(false)
          return
        }
        result = await queryFacts(storeName, {
          mode: "query",
          filters,
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
  }, [storeName, mode, tagInputs, subject, type, queryFilters, limit, direction])

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      {/* Filter panel */}
      <div className="rounded-xl border border-border bg-card p-5 space-y-4">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <h2 className="font-semibold text-sm">Query</h2>
          <div className="flex items-center gap-2 flex-wrap">
            {/* Mode selector */}
            <div className="flex rounded-lg border border-border overflow-hidden text-xs">
              {(["all", "tags", "subject", "type", "query"] as QueryMode[]).map((m) => (
                <button
                  key={m}
                  className={`px-3 py-1.5 font-medium transition-colors ${
                    mode === m
                      ? "bg-foreground text-background"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                  onClick={() => setMode(m)}
                >
                  {MODE_LABELS[m]}
                </button>
              ))}
            </div>
          </div>
        </div>


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
              <div key={tag.id} className="grid grid-cols-[1fr_auto_1fr_auto] items-center gap-2">
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
              onClick={() => setTagInputs([...tagInputs, { id: tagIdRef.current++, key: "", value: "" }])}
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

        {/* Type controls */}
        {mode === "type" && (
          <div className="space-y-1.5">
            <Label className="text-xs">Fact type</Label>
            <Input
              value={type}
              onChange={(e) => setType(e.target.value)}
              placeholder="e.g. OrderPlaced, com.acme.OrderPlaced"
              className="font-mono text-xs h-8 max-w-sm"
            />
            <p className="text-xs text-muted-foreground">Matched exactly.</p>
          </div>
        )}

        {/* Query controls */}
        {mode === "query" && (
          <div className="space-y-3">
            <p className="text-xs text-muted-foreground">
              A fact matches when it matches any filter. Within a filter, subjects, types and tags must all
              hold; subjects and types are comma-separated and match any of their values.
            </p>
            {queryFilters.map((filter, i) => (
              <div key={filter.id} className="rounded-lg border border-border p-3 space-y-2">
                <div className="flex items-center justify-between">
                  <Label className="text-xs">Filter {i + 1}</Label>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    disabled={queryFilters.length === 1}
                    onClick={() => setQueryFilters(queryFilters.filter((_, j) => j !== i))}
                  >
                    <X className="size-3.5" />
                  </Button>
                </div>
                <div className="grid gap-2 md:grid-cols-3">
                  <Input
                    value={filter.subjects}
                    onChange={(e) => updateFilter(i, { subjects: e.target.value })}
                    placeholder="subjects: order/1, order/2"
                    className="font-mono text-xs h-8"
                  />
                  <Input
                    value={filter.types}
                    onChange={(e) => updateFilter(i, { types: e.target.value })}
                    placeholder="types: OrderPlaced, OrderPaid"
                    className="font-mono text-xs h-8"
                  />
                  <Input
                    value={filter.tags}
                    onChange={(e) => updateFilter(i, { tags: e.target.value })}
                    placeholder="tags: region=eu, tier=gold"
                    className="font-mono text-xs h-8"
                  />
                </div>
              </div>
            ))}
            <Button
              variant="ghost"
              size="sm"
              className="text-xs h-7"
              onClick={() =>
                setQueryFilters([...queryFilters, { id: filterIdRef.current++, subjects: "", types: "", tags: "" }])
              }
            >
              <Plus className="size-3" />
              Add filter
            </Button>
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
