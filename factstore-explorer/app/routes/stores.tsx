import { useState, useEffect } from "react"
import { Link, useFetcher, useRevalidator } from "react-router"
import {
  Database,
  Plus,
  Calendar,
  AlertCircle,
  RefreshCw,
} from "lucide-react"
import { Button } from "~/components/ui/button"
import { Input } from "~/components/ui/input"
import { Label } from "~/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "~/components/ui/dialog"
import { Alert, AlertDescription } from "~/components/ui/alert"
import { listStores, createStore } from "~/lib/api"
import type { Route } from "./+types/stores"

export function meta(_: Route.MetaArgs) {
  return [{ title: "Stores — FactStore Explorer" }]
}

// ─── Loader ──────────────────────────────────────────────────────────────────

export async function clientLoader() {
  return { stores: await listStores() }
}

clientLoader.hydrate = true as const

// ─── Action ──────────────────────────────────────────────────────────────────

export async function clientAction({ request }: Route.ClientActionArgs) {
  const formData = await request.formData()
  const name = String(formData.get("name")).trim()
  try {
    await createStore(name)
    return { ok: true as const }
  } catch (err) {
    return { error: err instanceof Error ? err.message : "Failed to create store" }
  }
}

// ─── Create dialog ────────────────────────────────────────────────────────────

function CreateStoreDialog({
  open,
  onClose,
}: {
  open: boolean
  onClose: () => void
}) {
  const fetcher = useFetcher<typeof clientAction>()
  const [name, setName] = useState("")
  const isCreating = fetcher.state !== "idle"
  const error = fetcher.data && "error" in fetcher.data ? fetcher.data.error : null

  // Close and reset when action succeeds
  useEffect(() => {
    if (fetcher.state === "idle" && fetcher.data && "ok" in fetcher.data) {
      setName("")
      onClose()
    }
  }, [fetcher.state, fetcher.data, onClose])

  function handleClose() {
    setName("")
    onClose()
  }

  return (
    <Dialog open={open} onOpenChange={(o) => !o && handleClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Create Store</DialogTitle>
          <DialogDescription>
            Give your new fact store a unique name.
          </DialogDescription>
        </DialogHeader>
        <fetcher.Form method="post" className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="store-name">Name</Label>
            <Input
              id="store-name"
              name="name"
              placeholder="e.g. orders, events, audit-log"
              value={name}
              onChange={(e) => setName(e.target.value)}
              autoFocus
              pattern="[a-zA-Z0-9_\-]+"
              required
            />
            <p className="text-xs text-muted-foreground">
              Letters, digits, hyphens and underscores only.
            </p>
          </div>
          {error && (
            <Alert variant="destructive">
              <AlertCircle className="size-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={handleClose} disabled={isCreating}>
              Cancel
            </Button>
            <Button type="submit" disabled={isCreating || !name.trim()}>
              {isCreating ? "Creating…" : "Create"}
            </Button>
          </DialogFooter>
        </fetcher.Form>
      </DialogContent>
    </Dialog>
  )
}

// ─── Store card ───────────────────────────────────────────────────────────────

function StoreCard({ store }: { store: { id: string; name: string; createdAt: string } }) {
  const created = new Date(store.createdAt)

  return (
    <div className="group relative flex flex-col gap-3 rounded-xl border border-border bg-card p-5 transition-shadow hover:shadow-sm">
      <div className="flex items-center gap-2.5 min-w-0">
        <div className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-muted">
          <Database className="size-4 text-muted-foreground" />
        </div>
        <div className="min-w-0">
          <p className="truncate font-semibold text-sm">{store.name}</p>
          <p className="font-mono text-xs text-muted-foreground truncate">
            {store.id.slice(0, 8)}…
          </p>
        </div>
      </div>

      <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
        <Calendar className="size-3" />
        <span>Created {created.toLocaleDateString()}</span>
      </div>

      <div className="relative z-10 flex gap-2 pt-1 border-t border-border">
        <Button asChild variant="ghost" size="sm" className="flex-1 text-xs">
          <Link to={`/stores/${store.name}/facts`}>Browse Facts</Link>
        </Button>
        <Button asChild variant="ghost" size="sm" className="flex-1 text-xs">
          <Link to={`/stores/${store.name}/stream`}>Live Stream</Link>
        </Button>
      </div>

      <Link
        to={`/stores/${store.name}`}
        className="absolute inset-0 rounded-xl"
        aria-hidden
        tabIndex={-1}
      />
    </div>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function StoresPage({ loaderData }: Route.ComponentProps) {
  const { stores } = loaderData
  const { revalidate, state: revalidateState } = useRevalidator()
  const [showCreate, setShowCreate] = useState(false)

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
      <div className="mb-8 flex items-center justify-between gap-4">
        <div>
          <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1">
            FactStore Explorer
          </p>
          <h1 className="text-2xl font-bold tracking-tight">Stores</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Manage your fact stores and explore their contents.
          </p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Button
            variant="outline"
            size="sm"
            onClick={revalidate}
            disabled={revalidateState === "loading"}
          >
            <RefreshCw className={`size-3.5 ${revalidateState === "loading" ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button size="sm" onClick={() => setShowCreate(true)}>
            <Plus className="size-3.5" />
            New Store
          </Button>
        </div>
      </div>

      {stores.length === 0 ? (
        <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-border py-20 gap-4">
          <div className="flex size-12 items-center justify-center rounded-xl bg-muted">
            <Database className="size-6 text-muted-foreground" />
          </div>
          <div className="text-center">
            <p className="font-medium text-sm">No stores yet</p>
            <p className="text-sm text-muted-foreground mt-1">
              Create your first store to get started.
            </p>
          </div>
          <Button size="sm" onClick={() => setShowCreate(true)}>
            <Plus className="size-3.5" />
            Create Store
          </Button>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {stores.map((store) => (
            <StoreCard key={store.id} store={store} />
          ))}
        </div>
      )}

      <CreateStoreDialog open={showCreate} onClose={() => setShowCreate(false)} />
    </div>
  )
}
