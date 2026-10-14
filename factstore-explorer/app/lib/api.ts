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

/**
 * A failure the server reported, as an HTTP error response or as the `error` line of a stream,
 * which has no status of its own.
 */
export class ApiRequestError extends Error {
  readonly reason?: string
  readonly status?: number

  constructor(message: string, reason?: string, status?: number) {
    super(message)
    this.name = "ApiRequestError"
    this.reason = reason
    this.status = status
  }

  /**
   * Whether repeating the request unchanged is bound to fail again. A client error is, except
   * for a timeout or a rate limit, which a proxy may answer with and which pass with time.
   */
  get permanent(): boolean {
    if (this.reason) return this.reason !== "InternalError"
    if (this.status === 408 || this.status === 429) return false
    return this.status !== undefined && this.status >= 400 && this.status < 500
  }
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

/** What to read: each mode names the one thing it filters by, so none can be left out. */
export type QueryOptions = {
  limit?: number
  direction?: "forward" | "backward"
} & (
  | { mode: "all" }
  | { mode: "tags"; tags: string[] }
  | { mode: "subject"; subject: string }
  | { mode: "type"; type: string }
  | { mode: "query"; filters: FactFilter[] }
)

export async function queryFacts(storeName: string, opts: QueryOptions): Promise<Fact[]> {
  // A query carries everything in its body, and is posted to an operation on the facts.
  if (opts.mode === "query") {
    if (opts.filters.length === 0) throw new Error("A query needs at least one filter.")
    return fetchFactStream(
      `${BASE_URL}/v1/stores/${encodeURIComponent(storeName)}/facts:query`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filters: opts.filters,
          direction: opts.direction,
          ...(opts.limit && opts.limit > 0 ? { limit: opts.limit } : {}),
        }),
      },
    )
  }

  const params = new URLSearchParams()

  // An empty filter must not be dropped: without it the request would read every fact.
  if (opts.mode === "tags") {
    if (opts.tags.length === 0) throw new Error("A tag query needs at least one tag.")
    for (const tag of opts.tags) params.append("tag", tag)
  }
  if (opts.mode === "subject") {
    if (!opts.subject) throw new Error("A subject query needs a subject.")
    params.set("subject", opts.subject)
  }
  if (opts.mode === "type") {
    if (!opts.type) throw new Error("A type query needs a type.")
    params.set("type", opts.type)
  }

  if (opts.limit && opts.limit > 0) params.set("limit", String(opts.limit))
  if (opts.direction) params.set("direction", opts.direction)

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
  const facts: Fact[] = []
  for await (const line of readFactStreamLines(url, init)) {
    if ("fact" in line) {
      facts.push(line.fact)
    } else if ("end" in line) {
      if (line.end.count !== facts.length) {
        throw new Error(`The fact stream announced ${line.end.count} facts but delivered ${facts.length}.`)
      }
      return facts
    }
  }
  throw new Error("The fact stream ended before it was complete.")
}

/**
 * Opens an NDJSON fact stream and yields its lines as they arrive, until the response ends.
 *
 * Failures are thrown rather than yielded: an error response, and an `error` line, which a
 * stream sends in place of the lines it could not write.
 */
async function* readFactStreamLines(
  url: string,
  init: RequestInit = {},
): AsyncGenerator<Exclude<FactStreamLine, { error: ApiError }>> {
  const res = await fetch(url, {
    ...init,
    headers: { Accept: "application/x-ndjson", ...(init.headers ?? {}) },
  })
  if (!res.ok) throw await toApiError(res)
  if (!res.body) throw new Error("The fact stream has no body.")

  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  try {
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
        if ("error" in parsed) throw new ApiRequestError(parsed.error.message, parsed.error.reason)
        yield parsed
      }
      if (done) return
    }
  } finally {
    // A caller that stops early, at the `end` line or on a failure, no longer needs the body.
    reader.cancel().catch(() => {})
  }
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
 * A connection that drops is re-opened automatically. Once a fact has arrived, it continues
 * after the last one, so no fact is seen twice and none is skipped. Until then it starts from
 * the requested position again: from `end`, the facts appended while disconnected are not seen.
 *
 * `onError` is called, and the subscription is over, once the server reports a failure that
 * reconnecting cannot fix, such as an unknown store, or once [RECONNECT_ATTEMPTS] attempts in
 * a row have failed. An attempt has succeeded as soon as it receives a line.
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
      // Every response counts as an attempt, and every line it delivers proves the connection:
      // a response that ends without a single line, say from a proxy, fails like any other.
      let failure: unknown = new Error("The subscription ended without delivering anything.")
      try {
        await readSubscription(
          url(),
          controller.signal,
          (fact) => {
            lastFactId = fact.id
            onFact(fact)
          },
          // A heartbeat proves the connection as much as a fact does.
          () => {
            attempt = 0
          },
        )
        // The response ended although a subscription does not: reconnect.
      } catch (err) {
        failure = err
      }
      if (closed) return
      attempt += 1
      const permanent = failure instanceof ApiRequestError && failure.permanent
      if (permanent || attempt >= RECONNECT_ATTEMPTS) {
        onError(failure instanceof Error ? failure : new Error(String(failure)))
        return
      }
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

/**
 * Reads one subscription response, handing every `fact` line to [onFact]. [onLine] is told of
 * every line that shows the subscription being served: a fact or a heartbeat.
 */
async function readSubscription(
  url: string,
  signal: AbortSignal,
  onFact: (fact: Fact) => void,
  onLine: () => void,
): Promise<void> {
  for await (const line of readFactStreamLines(url, { signal })) {
    onLine()
    if ("fact" in line) onFact(line.fact)
    // A heartbeat only says the subscription is still being served.
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async function toApiError(res: Response): Promise<Error> {
  try {
    const body = (await res.json()) as ApiError
    return new ApiRequestError(body.message ?? `HTTP ${res.status}`, body.reason, res.status)
  } catch {
    return new ApiRequestError(`HTTP ${res.status} ${res.statusText}`, undefined, res.status)
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
