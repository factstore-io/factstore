import { useState, useEffect } from "react"
import { Link } from "react-router"
import {
  Database,
  Plus,
  Server,
  HardDrive,
  Tag,
  CheckCircle2,
  XCircle,
  Loader2,
  ArrowRight,
} from "lucide-react"
import { Button } from "~/components/ui/button"
import { Skeleton } from "~/components/ui/skeleton"
import { Badge } from "~/components/ui/badge"
import { getServerInfo, listStores, type ServerInfo, type StoreMetadata } from "~/lib/api"

export function meta() {
  return [{ title: "FactStore Explorer" }]
}

type Status = "loading" | "ok" | "error"

function StatusBadge({ status }: { status: Status }) {
  if (status === "loading")
    return (
      <Badge variant="secondary" className="gap-1.5">
        <Loader2 className="size-3 animate-spin" />
        Connecting…
      </Badge>
    )
  if (status === "ok")
    return (
      <Badge className="bg-green-500/10 text-green-600 dark:text-green-400 border border-green-500/20 gap-1.5">
        <CheckCircle2 className="size-3" />
        Connected
      </Badge>
    )
  return (
    <Badge className="bg-destructive/10 text-destructive border border-destructive/20 gap-1.5">
      <XCircle className="size-3" />
      Unreachable
    </Badge>
  )
}

function InfoTile({
  icon: Icon,
  label,
  value,
  loading,
}: {
  icon: React.ElementType
  label: string
  value: string | number | undefined
  loading: boolean
}) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-border bg-muted/30 px-4 py-3">
      <Icon className="size-4 text-muted-foreground shrink-0" />
      <div className="min-w-0">
        <p className="text-xs text-muted-foreground">{label}</p>
        {loading ? (
          <Skeleton className="mt-1 h-4 w-24" />
        ) : (
          <p className="text-sm font-medium truncate">{value ?? "—"}</p>
        )}
      </div>
    </div>
  )
}

export default function LandingPage() {
  const [status, setStatus] = useState<Status>("loading")
  const [info, setInfo] = useState<ServerInfo | null>(null)
  const [stores, setStores] = useState<StoreMetadata[]>([])
  const [storesLoading, setStoresLoading] = useState(true)

  useEffect(() => {
    let cancelled = false

    getServerInfo()
      .then((i) => { if (!cancelled) { setInfo(i); setStatus("ok") } })
      .catch(() => { if (!cancelled) setStatus("error") })

    listStores()
      .then((s) => { if (!cancelled) setStores(s) })
      .catch(() => {})
      .finally(() => { if (!cancelled) setStoresLoading(false) })

    return () => { cancelled = true }
  }, [])

  return (
    <div className="mx-auto max-w-2xl px-4 py-16 space-y-10">
      {/* Hero */}
      <div className="space-y-3">
        <div className="flex items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-xl bg-foreground">
            <Database className="size-5 text-background" />
          </div>
          <h1 className="text-2xl font-bold tracking-tight">FactStore Explorer</h1>
        </div>
        <p className="text-sm text-muted-foreground max-w-md">
          Inspect, query, and stream facts across your stores. Connect to a running
          FactStore server to get started.
        </p>
      </div>

      {/* Server status card */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="flex items-center justify-between gap-4 px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">Server</h2>
          <StatusBadge status={status} />
        </div>
        <div className="grid grid-cols-1 gap-3 p-5 sm:grid-cols-3">
          <InfoTile
            icon={Server}
            label="Version"
            value={info?.version}
            loading={status === "loading"}
          />
          <InfoTile
            icon={HardDrive}
            label="Storage backend"
            value={info?.storageBackend}
            loading={status === "loading"}
          />
          <InfoTile
            icon={Database}
            label="Stores"
            value={storesLoading ? undefined : stores.length}
            loading={storesLoading}
          />
        </div>
        {status === "error" && (
          <p className="px-5 pb-4 text-xs text-muted-foreground">
            Could not reach the server at <span className="font-mono">/api/v1/info</span>.
            Make sure the FactStore server is running.
          </p>
        )}
      </div>

      {/* Recent stores */}
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold">Stores</h2>
          <Button asChild variant="ghost" size="sm" className="text-xs">
            <Link to="/stores">
              View all
              <ArrowRight className="size-3" />
            </Link>
          </Button>
        </div>

        {storesLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-12 rounded-lg" />)}
          </div>
        ) : stores.length === 0 ? (
          <div className="flex flex-col items-center gap-3 rounded-xl border border-dashed border-border py-10">
            <p className="text-sm text-muted-foreground">No stores yet.</p>
            <Button asChild size="sm">
              <Link to="/stores">
                <Plus className="size-3.5" />
                Create your first store
              </Link>
            </Button>
          </div>
        ) : (
          <div className="divide-y divide-border rounded-xl border border-border overflow-hidden">
            {stores.slice(0, 5).map((store) => (
              <Link
                key={store.id}
                to={`/stores/${store.name}`}
                className="flex items-center justify-between gap-4 px-4 py-3 bg-card hover:bg-muted/50 transition-colors group"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <Database className="size-3.5 text-muted-foreground shrink-0" />
                  <span className="text-sm font-medium truncate">{store.name}</span>
                  <span className="font-mono text-xs text-muted-foreground hidden sm:inline">
                    {store.id.slice(0, 8)}…
                  </span>
                </div>
                <ArrowRight className="size-3.5 text-muted-foreground group-hover:text-foreground transition-colors shrink-0" />
              </Link>
            ))}
            {stores.length > 5 && (
              <Link
                to="/stores"
                className="flex items-center justify-center gap-1.5 px-4 py-2.5 bg-card hover:bg-muted/50 transition-colors text-xs text-muted-foreground"
              >
                +{stores.length - 5} more
                <ArrowRight className="size-3" />
              </Link>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
