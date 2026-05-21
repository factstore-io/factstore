import { useState } from "react"
import { Link, redirect, useFetcher } from "react-router"
import { Search, Radio, Tag, AlertCircle, ArrowRight, Trash2 } from "lucide-react"
import { Button } from "~/components/ui/button"
import { Alert, AlertDescription } from "~/components/ui/alert"
import { Badge } from "~/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "~/components/ui/dialog"
import { listStores, deleteStore } from "~/lib/api"
import type { Route } from "./+types/stores.$storeName._index"

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.storeName} — FactStore Explorer` }]
}

// ─── Loader ──────────────────────────────────────────────────────────────────

export async function clientLoader({ params }: Route.ClientLoaderArgs) {
  const stores = await listStores()
  return { store: stores.find((s) => s.name === params.storeName) ?? null }
}

clientLoader.hydrate = true as const

// ─── Action ──────────────────────────────────────────────────────────────────

export async function clientAction({ params }: Route.ClientActionArgs) {
  try {
    await deleteStore(params.storeName!)
    return redirect("/stores")
  } catch (err) {
    return { error: err instanceof Error ? err.message : "Failed to delete store" }
  }
}

// ─── Delete dialog ────────────────────────────────────────────────────────────

function DeleteDialog({
  storeName,
  open,
  onClose,
}: {
  storeName: string
  open: boolean
  onClose: () => void
}) {
  const fetcher = useFetcher<typeof clientAction>()
  const isDeleting = fetcher.state !== "idle"
  const error = fetcher.data && "error" in fetcher.data ? fetcher.data.error : null

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Delete Store</DialogTitle>
          <DialogDescription>
            This will permanently delete{" "}
            <span className="font-semibold font-mono">{storeName}</span> and all
            its facts. This action cannot be undone.
          </DialogDescription>
        </DialogHeader>
        {error && (
          <Alert variant="destructive">
            <AlertCircle className="size-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        <fetcher.Form method="post">
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={onClose} disabled={isDeleting}>
              Cancel
            </Button>
            <Button type="submit" variant="destructive" disabled={isDeleting}>
              {isDeleting ? "Deleting…" : "Delete Store"}
            </Button>
          </DialogFooter>
        </fetcher.Form>
      </DialogContent>
    </Dialog>
  )
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function InfoRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-4 py-3 border-b border-border last:border-0">
      <span className="w-32 shrink-0 text-xs font-mono uppercase tracking-widest text-muted-foreground pt-0.5">
        {label}
      </span>
      <span className="text-sm">{children}</span>
    </div>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function StoreIndexPage({ loaderData, params }: Route.ComponentProps) {
  const { store } = loaderData
  const storeName = params.storeName
  const [showDelete, setShowDelete] = useState(false)

  return (
    <div className="mx-auto max-w-3xl px-4 py-8 space-y-8">
      <div>
        <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1">
          Store
        </p>
        <h1 className="text-2xl font-bold tracking-tight font-mono">{storeName}</h1>
      </div>

      {/* Store metadata */}
      <div className="rounded-xl border border-border bg-card">
        <div className="px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">Details</h2>
        </div>
        <div className="px-5">
          {store ? (
            <>
              <InfoRow label="Name">
                <span className="font-mono">{store.name}</span>
              </InfoRow>
              <InfoRow label="ID">
                <span className="font-mono text-muted-foreground">{store.id}</span>
              </InfoRow>
              <InfoRow label="Created">
                <span>{new Date(store.createdAt).toLocaleString()}</span>
              </InfoRow>
            </>
          ) : (
            <p className="py-4 text-sm text-muted-foreground">Store not found.</p>
          )}
        </div>
      </div>

      {/* Quick actions */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <Link
          to={`/stores/${storeName}/facts`}
          className="group flex flex-col gap-3 rounded-xl border border-border bg-card p-5 transition-shadow hover:shadow-sm hover:border-foreground/20"
        >
          <div className="flex items-center justify-between">
            <div className="flex size-9 items-center justify-center rounded-lg bg-muted">
              <Search className="size-4 text-muted-foreground" />
            </div>
            <ArrowRight className="size-4 text-muted-foreground group-hover:text-foreground transition-colors" />
          </div>
          <div>
            <p className="font-semibold text-sm">Browse Facts</p>
            <p className="text-xs text-muted-foreground mt-0.5">
              Query facts by time range, tags, or subject.
            </p>
          </div>
        </Link>

        <Link
          to={`/stores/${storeName}/stream`}
          className="group flex flex-col gap-3 rounded-xl border border-border bg-card p-5 transition-shadow hover:shadow-sm hover:border-foreground/20"
        >
          <div className="flex items-center justify-between">
            <div className="flex size-9 items-center justify-center rounded-lg bg-muted">
              <Radio className="size-4 text-muted-foreground" />
            </div>
            <ArrowRight className="size-4 text-muted-foreground group-hover:text-foreground transition-colors" />
          </div>
          <div>
            <p className="font-semibold text-sm">Live Stream</p>
            <p className="text-xs text-muted-foreground mt-0.5">
              Stream facts in real time as they are appended.
            </p>
          </div>
        </Link>
      </div>

      {/* Coming soon section */}
      <div className="rounded-xl border border-border bg-card">
        <div className="px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">Explore</h2>
        </div>
        <div className="px-5 py-3">
          <div className="flex items-center justify-between py-2">
            <div className="flex items-center gap-3">
              <Tag className="size-4 text-muted-foreground" />
              <div>
                <p className="text-sm font-medium">Fact Types</p>
                <p className="text-xs text-muted-foreground">
                  Browse and inspect registered fact types.
                </p>
              </div>
            </div>
            <Badge variant="secondary" className="text-xs shrink-0">
              Coming soon
            </Badge>
          </div>
        </div>
      </div>

      {/* Danger zone */}
      <div className="rounded-xl border border-destructive/30 bg-card">
        <div className="px-5 py-4 border-b border-destructive/30">
          <h2 className="text-sm font-semibold text-destructive">Danger Zone</h2>
        </div>
        <div className="px-5 py-4 flex items-center justify-between gap-4">
          <div>
            <p className="text-sm font-medium">Delete this store</p>
            <p className="text-xs text-muted-foreground mt-0.5">
              Permanently removes the store and all its facts. This cannot be undone.
            </p>
          </div>
          <Button
            variant="destructive"
            size="sm"
            className="shrink-0"
            onClick={() => setShowDelete(true)}
          >
            <Trash2 className="size-3.5" />
            Delete Store
          </Button>
        </div>
      </div>

      <DeleteDialog
        storeName={storeName}
        open={showDelete}
        onClose={() => setShowDelete(false)}
      />
    </div>
  )
}
