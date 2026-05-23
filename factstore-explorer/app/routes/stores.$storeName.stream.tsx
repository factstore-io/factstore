import { useState, useEffect, useRef, useCallback } from "react"
import { useParams } from "react-router"
import { Play, Pause, Trash2, AlertCircle, WifiOff } from "lucide-react"
import type { Route } from "./+types/stores.$storeName.stream"
import { Button } from "~/components/ui/button"
import { Input } from "~/components/ui/input"
import { Label } from "~/components/ui/label"
import { Badge } from "~/components/ui/badge"
import { Alert, AlertDescription } from "~/components/ui/alert"
import { FactTable } from "~/components/FactTable"
import { createFactStream, type Fact, type StreamPosition } from "~/lib/api"

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `Stream — ${params.storeName} — FactStore Explorer` }]
}

type StartMode = "end" | "beginning" | "after"

const MAX_BUFFERED = 500

export default function StreamPage() {
  const { storeName } = useParams<{ storeName: string }>()

  const [startMode, setStartMode] = useState<StartMode>("end")
  const [afterId, setAfterId] = useState("")
  const [running, setRunning] = useState(false)
  const [facts, setFacts] = useState<Fact[]>([])
  const [error, setError] = useState<string | null>(null)
  const [totalReceived, setTotalReceived] = useState(0)
  const [frozen, setFrozen] = useState(false)
  const [pendingCount, setPendingCount] = useState(0)

  const esRef = useRef<EventSource | null>(null)
  const pendingRef = useRef<Fact[]>([])
  // Ref keeps onFact callback current without restarting the stream
  const frozenRef = useRef(false)

  useEffect(() => {
    frozenRef.current = frozen
  }, [frozen])

  const stop = useCallback(() => {
    esRef.current?.close()
    esRef.current = null
    setRunning(false)
  }, [])

  const start = useCallback(() => {
    if (!storeName) return
    stop()
    setError(null)
    pendingRef.current = []
    setPendingCount(0)

    const position: StreamPosition =
      startMode === "end"
        ? "end"
        : startMode === "beginning"
        ? "beginning"
        : { after: afterId.trim() }

    const es = createFactStream(
      storeName,
      position,
      (fact) => {
        setTotalReceived((n) => n + 1)
        if (!frozenRef.current) {
          setFacts((prev) => {
            const next = [fact, ...prev]
            return next.length > MAX_BUFFERED ? next.slice(0, MAX_BUFFERED) : next
          })
        } else {
          pendingRef.current = [fact, ...pendingRef.current]
          setPendingCount((n) => n + 1)
        }
      },
      (err) => {
        setError(err.message)
        setRunning(false)
      },
    )

    esRef.current = es
    setRunning(true)
    setFacts([])
    setTotalReceived(0)
  }, [storeName, startMode, afterId, stop])

  function handleToggleFreeze() {
    if (frozen) {
      // Flush buffered facts into the display
      const pending = pendingRef.current
      pendingRef.current = []
      setPendingCount(0)
      if (pending.length > 0) {
        setFacts((prev) => {
          const combined = [...pending, ...prev]
          return combined.length > MAX_BUFFERED ? combined.slice(0, MAX_BUFFERED) : combined
        })
      }
      setFrozen(false)
    } else {
      setFrozen(true)
    }
  }

  // Cleanup on unmount
  useEffect(() => () => stop(), [stop])

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      {/* Stream config */}
      <div className="rounded-xl border border-border bg-card p-5 space-y-4">
        <div className="flex items-center justify-between gap-4">
          <h2 className="font-semibold text-sm">Live Stream</h2>
          <div className="flex items-center gap-2">
            {running ? (
              <Badge className="bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20 border gap-1.5">
                <span className="size-1.5 rounded-full bg-green-500 animate-pulse inline-block" />
                Connected
              </Badge>
            ) : (
              <Badge variant="secondary" className="gap-1.5">
                <WifiOff className="size-3" />
                Disconnected
              </Badge>
            )}
          </div>
        </div>

        <div className="flex flex-wrap gap-4 items-end">
          <div className="space-y-1.5">
            <Label className="text-xs">Start position</Label>
            <div className="flex rounded-lg border border-border overflow-hidden text-xs">
              {(["end", "beginning", "after"] as StartMode[]).map((m) => (
                <button
                  key={m}
                  disabled={running}
                  className={`px-3 py-1.5 font-medium transition-colors disabled:opacity-50 ${
                    startMode === m
                      ? "bg-foreground text-background"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                  onClick={() => setStartMode(m)}
                >
                  {m === "end" ? "Tail (live)" : m === "beginning" ? "From start" : "After ID"}
                </button>
              ))}
            </div>
          </div>

          {startMode === "after" && (
            <div className="space-y-1.5">
              <Label className="text-xs">Fact ID</Label>
              <Input
                value={afterId}
                onChange={(e) => setAfterId(e.target.value)}
                placeholder="UUID of the last known fact"
                className="font-mono text-xs h-8 w-72"
                disabled={running}
              />
            </div>
          )}

          <div className="flex items-center gap-2 ml-auto">
            {running ? (
              <Button variant="outline" size="sm" onClick={stop}>
                <Pause className="size-3.5" />
                Disconnect
              </Button>
            ) : (
              <Button size="sm" onClick={start}>
                <Play className="size-3.5" />
                Connect
              </Button>
            )}
          </div>
        </div>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="size-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Stream output */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="flex items-center justify-between gap-4 px-5 py-3 border-b border-border">
          <div className="flex items-center gap-3">
            <span className="text-xs font-mono text-muted-foreground">
              {facts.length} displayed · {totalReceived} received
            </span>
            {frozen && pendingCount > 0 && (
              <Badge variant="secondary" className="text-xs">
                {pendingCount} buffered
              </Badge>
            )}
            {!frozen && facts.length >= MAX_BUFFERED && (
              <Badge variant="secondary" className="text-xs">
                Buffer full (last {MAX_BUFFERED})
              </Badge>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              className="text-xs h-7"
              onClick={handleToggleFreeze}
            >
              {frozen ? (
                <>
                  <Play className="size-3" />
                  Resume{pendingCount > 0 ? ` (${pendingCount})` : ""}
                </>
              ) : (
                <>
                  <Pause className="size-3" />
                  Freeze
                </>
              )}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="text-xs h-7 text-muted-foreground"
              onClick={() => {
                pendingRef.current = []
                setPendingCount(0)
                setFacts([])
                setTotalReceived(0)
              }}
            >
              <Trash2 className="size-3" />
              Clear
            </Button>
          </div>
        </div>

        <FactTable
          facts={facts}
          highlightNew
          emptyMessage={
            running ? "Waiting for facts…" : "Connect to start receiving facts."
          }
        />
      </div>
    </div>
  )
}
