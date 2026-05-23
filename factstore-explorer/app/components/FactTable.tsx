import { useState, useEffect } from "react"
import { ChevronDown, ChevronRight, Copy, Check } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table"
import { Badge } from "~/components/ui/badge"
import { Skeleton } from "~/components/ui/skeleton"
import { decodePayload, relativeTime, type Fact } from "~/lib/api"
import { cn } from "~/lib/utils"

const TYPE_COLORS = [
  "bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20",
  "bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20",
  "bg-purple-500/10 text-purple-600 dark:text-purple-400 border-purple-500/20",
  "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20",
  "bg-rose-500/10 text-rose-600 dark:text-rose-400 border-rose-500/20",
]

function getTypeColor(type: string): string {
  let hash = 0
  for (let i = 0; i < type.length; i++) {
    hash = (hash * 31 + type.charCodeAt(i)) & 0xffffffff
  }
  return TYPE_COLORS[Math.abs(hash) % TYPE_COLORS.length]
}

interface FactRowProps {
  readonly fact: Fact
  readonly highlightNew?: boolean
}

function FactRow({ fact, highlightNew }: FactRowProps) {
  const [expanded, setExpanded] = useState(false)
  const [copied, setCopied] = useState(false)

  function copyId(e: React.MouseEvent) {
    e.stopPropagation()
    navigator.clipboard.writeText(fact.id)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  const payload = decodePayload(fact.payload.data)
  let prettyPayload = payload
  try {
    prettyPayload = JSON.stringify(JSON.parse(payload), null, 2)
  } catch {
    // keep raw
  }

  const tags = fact.tags ? Object.entries(fact.tags) : []
  const metadata = fact.metadata ? Object.entries(fact.metadata) : []

  return (
    <>
      <TableRow
        className={cn("cursor-pointer group", highlightNew && "fact-arrive")}
        onClick={() => setExpanded((v) => !v)}
      >
        <TableCell className="w-5 pr-0">
          {expanded ? (
            <ChevronDown className="size-3.5 text-muted-foreground" />
          ) : (
            <ChevronRight className="size-3.5 text-muted-foreground group-hover:text-foreground transition-colors" />
          )}
        </TableCell>
        <TableCell className="font-mono text-xs text-muted-foreground">
          {fact.id?.slice(0, 8)}…
        </TableCell>
        <TableCell>
          <Badge
            variant="outline"
            className={cn("font-mono text-xs border", getTypeColor(fact.type))}
          >
            {fact.type}
          </Badge>
        </TableCell>
        <TableCell className="font-mono text-xs">{fact.subject}</TableCell>
        <TableCell className="text-xs text-muted-foreground whitespace-nowrap">
          <span title={fact.appendedAt}>{relativeTime(fact.appendedAt)}</span>
        </TableCell>
        <TableCell>
          <div className="flex flex-wrap gap-1">
            {tags.map(([k, v]) => (
              <span
                key={k}
                className="inline-flex items-center font-mono text-xs rounded-full border border-border px-2 py-0.5 text-muted-foreground"
              >
                <span>{k}</span>
                <span className="mx-1 opacity-40">=</span>
                <span className="text-foreground">{v}</span>
              </span>
            ))}
          </div>
        </TableCell>
      </TableRow>

      {expanded && (
        <TableRow className="bg-muted/30 hover:bg-muted/30">
          <TableCell colSpan={6} className="py-3 px-4">
            <div className="grid grid-cols-[1fr_1fr] gap-4 lg:grid-cols-[1fr_1fr_1fr]">
              <div>
                <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
                  Fact ID
                </p>
                <div className="flex items-center gap-1.5">
                  <p className="font-mono text-xs break-all">{fact.id}</p>
                  <button
                    onClick={copyId}
                    className="shrink-0 rounded p-0.5 text-muted-foreground transition-colors hover:text-foreground"
                    aria-label="Copy fact ID"
                  >
                    {copied
                      ? <Check className="size-3.5 text-green-500" />
                      : <Copy className="size-3.5" />}
                  </button>
                </div>
              </div>
              <div>
                <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
                  Appended At
                </p>
                <p className="font-mono text-xs">{fact.appendedAt}</p>
              </div>
              {metadata.length > 0 && (
                <div>
                  <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
                    Metadata
                  </p>
                  <div className="flex flex-wrap gap-1">
                    {metadata.map(([k, v]) => (
                      <span
                        key={k}
                        className="inline-flex items-center font-mono text-xs rounded-full border border-border px-2 py-0.5 text-muted-foreground"
                      >
                        {k}=<span className="text-foreground">{v}</span>
                      </span>
                    ))}
                  </div>
                </div>
              )}
              <div className="col-span-full">
                <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
                  Payload
                </p>
                <pre className="text-xs font-mono bg-background border border-border rounded-md p-3 overflow-x-auto max-h-48 whitespace-pre-wrap break-all">
                  {prettyPayload}
                </pre>
              </div>
            </div>
          </TableCell>
        </TableRow>
      )}
    </>
  )
}

interface FactTableProps {
  readonly facts: Fact[]
  readonly loading?: boolean
  readonly emptyMessage?: string
  readonly highlightNew?: boolean
}

function useTick(intervalMs: number) {
  const [_, setTick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => setTick((n) => n + 1), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
}

export function FactTable({ facts, loading, emptyMessage = "No facts found.", highlightNew }: FactTableProps) {
  useTick(30_000)
  if (loading) {
    return (
      <div className="space-y-2 p-4">
        {["sk-1", "sk-2", "sk-3", "sk-4", "sk-5"].map((id) => (
          <Skeleton key={id} className="h-10 w-full rounded-md" />
        ))}
      </div>
    )
  }

  if (facts.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
        <p className="text-sm">{emptyMessage}</p>
      </div>
    )
  }

  return (
    <div className="relative overflow-auto">
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead className="w-5" />
            <TableHead className="font-mono text-xs uppercase tracking-widest">ID</TableHead>
            <TableHead className="font-mono text-xs uppercase tracking-widest">Type</TableHead>
            <TableHead className="font-mono text-xs uppercase tracking-widest">Subject</TableHead>
            <TableHead className="font-mono text-xs uppercase tracking-widest whitespace-nowrap">Appended At</TableHead>
            <TableHead className="font-mono text-xs uppercase tracking-widest">Tags</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {facts.map((fact) => (
            <FactRow key={fact.id} fact={fact} highlightNew={highlightNew} />
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
