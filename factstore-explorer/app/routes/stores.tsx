import { useState, useEffect, useCallback } from "react"
import { Link } from "react-router"
import {
  Database,
  Plus,
  Calendar,
  ArrowRight,
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
import { Skeleton } from "~/components/ui/skeleton"
import { listStores, createStore, type StoreMetadata } from "~/lib/api"

export function meta() {
  return [{ title: "Stores — FactStore Explorer" }]
}

function CreateStoreDialog({
  open,
  onClose,
  onCreated,
}: {
  open: boolean
  onClose: () => void
  onCreated: () => void
}) {
  const [name, setName] = useState("")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function handleClose() {
    setName("")
    setError(null)
    onClose()
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!name.trim()) return
    setLoading(true)
    setError(null)
    try {
      await createStore(name.trim())
      handleClose()
      onCreated()
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create store")
    } finally {
      setLoading(false)
    }
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
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="store-name">Name</Label>
            <Input
              id="store-name"
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
            <Button type="button" variant="ghost" onClick={handleClose} disabled={loading}>
              Cancel
            </Button>
            <Button type="submit" disabled={loading || !name.trim()}>
              {loading ? "Creating…" : "Create"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

function StoreCard({ store }: { store: StoreMetadata }) {
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

      <div className="flex gap-2 pt-1 border-t border-border">
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

export default function StoresPage() {
  const [stores, setStores] = useState<StoreMetadata[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setStores(await listStores())
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load stores")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

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
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={`size-3.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button size="sm" onClick={() => setShowCreate(true)}>
            <Plus className="size-3.5" />
            New Store
          </Button>
        </div>
      </div>

      {error && (
        <Alert variant="destructive" className="mb-6">
          <AlertCircle className="size-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {loading ? (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-40 rounded-xl" />
          ))}
        </div>
      ) : stores.length === 0 ? (
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

      <CreateStoreDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        onCreated={load}
      />
    </div>
  )
}
