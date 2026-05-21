const BASE_URL = "/api"

// ─── Domain types ────────────────────────────────────────────────────────────

export interface StoreMetadata {
  id: string
  name: string
  createdAt: string
}

export interface Fact {
  id: string
  type: string
  subject: string
  appendedAt: string
  payload: { data: string }
  metadata: Record<string, string> | null
  tags: Record<string, string> | null
}

export interface ApiError {
  reason: string
  message: string
}

// ─── Server info ─────────────────────────────────────────────────────────────

export interface ServerInfo {
  app: string
  version: string
  storageBackend: string
}

export async function getServerInfo(): Promise<ServerInfo> {
  const res = await fetch(`${BASE_URL}/v1/info`)
  if (!res.ok) throw await toApiError(res)
  return res.json()
}

// ─── Stores ──────────────────────────────────────────────────────────────────

export async function listStores(): Promise<StoreMetadata[]> {
  const res = await fetch(`${BASE_URL}/v1/stores`)
  if (!res.ok) throw await toApiError(res)
  return res.json()
}

export async function createStore(name: string): Promise<{ id: string }> {
  const res = await fetch(`${BASE_URL}/v1/stores`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  })
  if (!res.ok) throw await toApiError(res)
  return res.json()
}

export async function deleteStore(name: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/v1/stores/${encodeURIComponent(name)}`, {
    method: "DELETE",
  })
  if (!res.ok) throw await toApiError(res)
}

// ─── Facts ───────────────────────────────────────────────────────────────────

export interface QueryOptions {
  mode: "timeRange" | "tags" | "subject"
  from?: string
  to?: string
  tags?: string[]
  subject?: string
  limit?: number
  direction?: "forward" | "backward"
}

export async function queryFacts(storeName: string, opts: QueryOptions): Promise<Fact[]> {
  const params = new URLSearchParams()

  if (opts.mode === "timeRange") {
    if (opts.from) params.set("from", opts.from)
    if (opts.to) params.set("to", opts.to)
  } else if (opts.mode === "tags" && opts.tags) {
    for (const tag of opts.tags) params.append("tag", tag)
  }

  if (opts.limit && opts.limit > 0) params.set("limit", String(opts.limit))
  if (opts.direction) params.set("direction", opts.direction)

  if (opts.mode === "subject" && opts.subject) {
    const url = `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/subjects/${encodeURIComponent(opts.subject)}/facts?${params}`
    const res = await fetch(url)
    if (!res.ok) throw await toApiError(res)
    return res.json()
  }

  const url = `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts?${params}`
  const res = await fetch(url)
  if (!res.ok) throw await toApiError(res)
  return res.json()
}

// ─── Streaming ───────────────────────────────────────────────────────────────

export type StreamPosition = "beginning" | "end" | { after: string }

export function createFactStream(
  storeName: string,
  position: StreamPosition,
  onFact: (fact: Fact) => void,
  onError: (err: Error) => void,
): EventSource {
  const params = new URLSearchParams()
  if (position === "beginning") {
    params.set("from", "beginning")
  } else if (position === "end") {
    params.set("from", "end")
  } else {
    params.set("after", position.after)
  }

  const url = `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts/stream?${params}`
  const es = new EventSource(url)

  es.onmessage = (e) => {
    try {
      onFact(JSON.parse(e.data) as Fact)
    } catch {
      // ignore parse errors
    }
  }

  es.onerror = () => {
    onError(new Error("Stream connection error"))
  }

  return es
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async function toApiError(res: Response): Promise<Error> {
  try {
    const body = (await res.json()) as ApiError
    return new Error(body.message ?? `HTTP ${res.status}`)
  } catch {
    return new Error(`HTTP ${res.status} ${res.statusText}`)
  }
}

export function decodePayload(data: string): string {
  try {
    return atob(data)
  } catch {
    return data
  }
}

export function relativeTime(isoString: string): string {
  const ms = Date.now() - new Date(isoString).getTime()
  if (ms < 1000) return "just now"
  if (ms < 60_000) return `${Math.floor(ms / 1000)}s ago`
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ago`
  if (ms < 86_400_000) return `${Math.floor(ms / 3_600_000)}h ago`
  return `${Math.floor(ms / 86_400_000)}d ago`
}
