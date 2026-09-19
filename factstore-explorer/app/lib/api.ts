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

  if (opts.mode === "subject" && opts.subject) params.set("subject", opts.subject)
  if (opts.mode === "type" && opts.type) params.set("type", opts.type)

  return fetchFactStream(`${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts?${params}`)
}

/**
 * One line of an NDJSON fact stream: a line per fact, then an `end` line once all facts
 * were sent, or an `error` line if the stream failed part way. A subscription never ends,
 * so it sends `heartbeat` lines instead of an `end` line while nothing is appended.
 */
type FactStreamLine =
  | { fact: Fact }
  | { end: { count: number } }
  | { heartbeat: { at: string } }
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

// ─── Subscriptions ───────────────────────────────────────────────────────────

export type SubscriptionPosition = "beginning" | "end" | { continueAfter: string }

export type FactSubscription = { close: () => void }

/** How often a dropped subscription is re-opened before the caller is told. */
const RECONNECT_ATTEMPTS = 5
const RECONNECT_DELAY_MS = 1000

/**
 * Subscribes to a store's facts over NDJSON: the facts it already holds, then the facts
 * appended next, until `close()` is called.
 *
 * A connection that drops is re-opened automatically and continues after the last fact that
 * arrived, so no fact is seen twice and none is skipped. `onError` is called once reconnecting
 * has failed [RECONNECT_ATTEMPTS] times in a row, after which the subscription is over.
 */
export function subscribeToFacts(
  storeName: string,
  position: SubscriptionPosition,
  onFact: (fact: Fact) => void,
  onError: (err: Error) => void,
): FactSubscription {
  const controller = new AbortController()
  let closed = false
  let lastFactId: string | null = null

  function url(): string {
    const params = new URLSearchParams()
    // Once a fact has arrived it is the position to resume from, whatever was asked for.
    if (lastFactId) params.set("continueAfter", lastFactId)
    else if (typeof position === "object") params.set("continueAfter", position.continueAfter)
    else params.set("from", position)
    return `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts:subscribe?${params}`
  }

  async function run() {
    let attempt = 0
    while (!closed) {
      try {
        await readSubscription(url(), controller.signal, (fact) => {
          attempt = 0
          lastFactId = fact.id
          onFact(fact)
        })
        // The response ended although a subscription does not: reconnect.
      } catch (err) {
        if (closed) return
        attempt += 1
        if (attempt >= RECONNECT_ATTEMPTS) {
          onError(err instanceof Error ? err : new Error(String(err)))
          return
        }
      }
      if (closed) return
      await new Promise((resolve) => setTimeout(resolve, RECONNECT_DELAY_MS))
    }
  }

  void run()

  return {
    close: () => {
      closed = true
      controller.abort()
    },
  }
}

/** Reads one subscription response, handing every `fact` line to [onFact]. */
async function readSubscription(
  url: string,
  signal: AbortSignal,
  onFact: (fact: Fact) => void,
): Promise<void> {
  const res = await fetch(url, { headers: { Accept: "application/x-ndjson" }, signal })
  if (!res.ok) throw await toApiError(res)
  if (!res.body) throw new Error("The subscription has no body.")

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
      if ("fact" in parsed) onFact(parsed.fact)
      else if ("error" in parsed) throw new Error(parsed.error.message)
      // A heartbeat only says the subscription is still being served.
    }
    if (done) return
  }
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
