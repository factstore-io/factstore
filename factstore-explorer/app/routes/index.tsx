import { Link } from "react-router"
import {
  Database,
  Plus,
  Server,
  HardDrive,
  CheckCircle2,
  XCircle,
  ArrowRight,
} from "lucide-react"
import { Button } from "~/components/ui/button"
import { Badge } from "~/components/ui/badge"
import { getServerInfo, listStores } from "~/lib/api"
import type { Route } from "./+types/index"

export function meta(_: Route.MetaArgs) {
  return [{ title: "FactStore Explorer" }]
}

export async function clientLoader() {
  const [infoResult, storesResult] = await Promise.allSettled([
    getServerInfo(),
    listStores(),
  ])
  return {
    info: infoResult.status === "fulfilled" ? infoResult.value : null,
    stores: storesResult.status === "fulfilled" ? storesResult.value : [],
  }
}

clientLoader.hydrate = true as const

function InfoTile({
  icon: Icon,
  label,
  value,
}: {
  readonly icon: React.ElementType
  readonly label: string
  readonly value: string | number | undefined
}) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-border bg-muted/30 px-4 py-3">
      <Icon className="size-4 text-muted-foreground shrink-0" />
      <div className="min-w-0">
        <p className="text-xs text-muted-foreground">{label}</p>
        <p className="text-sm font-medium truncate">{value ?? "—"}</p>
      </div>
    </div>
  )
}

export default function LandingPage({ loaderData }: Route.ComponentProps) {
  const { info, stores } = loaderData
  const connected = info !== null

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

      {/* Server status */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="flex items-center justify-between gap-4 px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">Server</h2>
          {connected ? (
            <Badge className="bg-green-500/10 text-green-600 dark:text-green-400 border border-green-500/20 gap-1.5">
              <CheckCircle2 className="size-3" />
              Connected
            </Badge>
          ) : (
            <Badge className="bg-destructive/10 text-destructive border border-destructive/20 gap-1.5">
              <XCircle className="size-3" />
              Unreachable
            </Badge>
          )}
        </div>
        <div className="grid grid-cols-1 gap-3 p-5 sm:grid-cols-3">
          <InfoTile icon={Server} label="Version" value={info?.version} />
          <InfoTile icon={HardDrive} label="Storage backend" value={info?.storageBackend} />
          <InfoTile icon={Database} label="Stores" value={stores.length} />
        </div>
        {!connected && (
          <p className="px-5 pb-4 text-xs text-muted-foreground">
            Could not reach the server at{" "}
            <span className="font-mono">/api/v1/info</span>. Make sure the
            FactStore server is running.
          </p>
        )}
      </div>

      {/* Stores */}
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

        {stores.length === 0 ? (
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
