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

export async function getStore(name: string): Promise<StoreMetadata> {
  const res = await fetch(`${BASE_URL}/v1/stores/${encodeURIComponent(name)}`)
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

/** One filter of a fact query: every property that is set must hold. */
export interface FactFilter {
  subjects?: string[]
  types?: string[]
  tags?: Record<string, string>
}

export interface QueryOptions {
  mode: "all" | "tags" | "subject" | "type" | "query"
  tags?: string[]
  subject?: string
  type?: string
  filters?: FactFilter[]
  limit?: number
  direction?: "forward" | "backward"
}

export async function queryFacts(storeName: string, opts: QueryOptions): Promise<Fact[]> {
  // A query carries everything in its body, and is posted to an operation on the facts.
  if (opts.mode === "query") {
    return fetchFactStream(
      `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts:query`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filters: opts.filters ?? [],
          direction: opts.direction,
          ...(opts.limit && opts.limit > 0 ? { limit: opts.limit } : {}),
        }),
      },
    )
  }

  const params = new URLSearchParams()

  if (opts.mode === "tags" && opts.tags) {
    for (const tag of opts.tags) params.append("tag", tag)
  }

  if (opts.limit && opts.limit > 0) params.set("limit", String(opts.limit))
  if (opts.direction) params.set("direction", opts.direction)

  if (opts.mode === "subject" && opts.subject) {
    return fetchFactStream(`${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/subjects/${encodeURIComponent(opts.subject)}/facts?${params}`)
  }

  if (opts.mode === "type" && opts.type) {
    return fetchFactStream(`${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/types/${encodeURIComponent(opts.type)}/facts?${params}`)
  }

  return fetchFactStream(`${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts?${params}`)
}

/**
 * One line of an NDJSON fact stream: a line per fact, then an `end` line once all facts
 * were sent, or an `error` line if the stream failed part way.
 */
type FactStreamLine =
  | { fact: Fact }
  | { end: { count: number } }
  | { error: ApiError }

/**
 * Reads an NDJSON fact stream line by line. Resolves only once the `end` line confirms
 * that every fact arrived; a stream that stops without it is reported as incomplete.
 */
async function fetchFactStream(url: string, init: RequestInit = {}): Promise<Fact[]> {
  const res = await fetch(url, {
    ...init,
    headers: { Accept: "application/x-ndjson", ...(init.headers ?? {}) },
  })
  if (!res.ok) throw await toApiError(res)
  if (!res.body) throw new Error("The fact stream has no body.")

  const facts: Fact[] = []
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  let pending = ""
  for (;;) {
    const { done, value } = await reader.read()
    if (value) pending += value
    const lines = pending.split("\n")
    // Until the body is done, the last piece may be a line that has not fully arrived yet.
    pending = done ? "" : (lines.pop() ?? "")
    for (const line of lines) {
      if (!line.trim()) continue
      const parsed = parseFactStreamLine(line)
      if ("fact" in parsed) {
        facts.push(parsed.fact)
      } else if ("end" in parsed) {
        if (parsed.end.count !== facts.length) {
          throw new Error(`The fact stream announced ${parsed.end.count} facts but delivered ${facts.length}.`)
        }
        return facts
      } else if ("error" in parsed) {
        throw new Error(parsed.error.message)
      }
    }
    if (done) break
  }
  throw new Error("The fact stream ended before it was complete.")
}

function parseFactStreamLine(line: string): FactStreamLine {
  try {
    return JSON.parse(line) as FactStreamLine
  } catch {
    // The server writes whole lines only, so a line that does not parse was cut off.
    throw new Error("The fact stream ended in the middle of a line.")
  }
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

  const url = `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts/subscribe?${params}`
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
