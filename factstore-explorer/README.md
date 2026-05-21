# FactStore Explorer

A web UI for inspecting, querying, and streaming facts across FactStore stores. It connects to a running `factstore-server` instance and exposes the full HTTP API through a browser interface.

**Features**

- Browse and manage stores (create, delete)
- Query facts by time range, tags, or subject
- Stream facts in real time via Server-Sent Events (SSE)
- Light and dark mode with system preference detection

---

## Getting Started

FactStore Explorer is a static SPA served by the `factstore-server` Quarkus application via [Quinoa](https://quarkiverse.github.io/quarkiverse-docs/quarkus-quinoa/dev/). In development, Quinoa starts the Vite dev server automatically alongside Quarkus Dev Mode.

**Prerequisites:** Node.js 20+, npm

### Development (recommended)

Run from the monorepo root. Quinoa handles launching the Vite dev server and proxying API calls:

```bash
./gradlew :factstore-server:quarkusDev
```

The UI is available at `http://localhost:8080` (served by Quarkus). The Vite dev server starts on port 3000 internally — do not open it directly, as API calls use a relative `/api` prefix that only resolves through the Quarkus proxy.

### Standalone (frontend only)

If you need to run the UI independently, configure a Vite proxy to forward `/api` to a running `factstore-server`, then:

```bash
npm install
npm run dev
```

### Typecheck

```bash
npm run typecheck
```

### Production build

Quinoa runs this automatically when packaging the Quarkus application:

```bash
npm run build
```

The output in `build/client/` is static HTML, JS, and CSS. Quinoa copies it into `factstore-server`'s classpath resources so Quarkus serves it directly.

---

## Architecture

### Technology stack

| Layer | Choice | Notes |
|---|---|---|
| Framework | React Router v7 (framework mode) | SPA mode — no SSR |
| Build tool | Vite 8 | Via `@react-router/dev` Vite plugin |
| Language | TypeScript 5 | |
| Styling | Tailwind CSS v4 | `@theme inline` with CSS custom properties |
| UI components | shadcn/ui (radix-nova style) | Radix UI primitives + `cn()` utility |
| Icons | lucide-react | |
| Streaming | Native `EventSource` | No extra library required |

### Project layout

```
app/
  app.css               # Tailwind v4 entry point, CSS custom properties, animations
  root.tsx              # App shell: header, breadcrumbs, dark mode toggle, HydrateFallback
  routes.ts             # Typed route config
  lib/
    api.ts              # All HTTP calls, domain types, streaming factory, payload helpers
    utils.ts            # cn() — clsx + tailwind-merge
  components/
    FactTable.tsx        # Expandable fact rows with copy-ID and arrival animation
    ui/                  # shadcn/ui generated components — do not edit manually
  routes/
    index.tsx                          # /  — landing: server status + store list
    stores.tsx                         # /stores — store list + create store
    stores.$storeName.tsx              # /stores/:storeName — tab layout
    stores.$storeName._index.tsx       # /stores/:storeName — overview + danger zone
    stores.$storeName.facts.tsx        # /stores/:storeName/facts — fact query browser
    stores.$storeName.stream.tsx       # /stores/:storeName/stream — live SSE stream
```

### SPA mode and data loading

The app runs in React Router's SPA mode (`ssr: false` in `vite.config.ts`). Quarkus serves a static HTML shell; all rendering happens in the browser.

**`clientLoader`** is the standard way to load data for a route. It runs when the user navigates to the route, and also on the initial page load when `clientLoader.hydrate = true as const` is set. Without that flag, a direct URL visit (e.g. bookmarking `/stores/my-store`) would render the component without data until the user navigated away and back.

```ts
export async function clientLoader({ params }: Route.ClientLoaderArgs) {
  return { stores: await listStores() }
}
clientLoader.hydrate = true as const
```

The root route (`root.tsx`) defines the single `HydrateFallback` (a centered spinner) shown while hydration-time loaders complete. SPA mode only permits `HydrateFallback` on the root — child routes must not export it.

**`clientAction`** handles mutations. After the action finishes, React Router automatically re-runs the current route's `clientLoader`, so displayed data always reflects server state without manual cache invalidation.

```ts
export async function clientAction({ params }: Route.ClientActionArgs) {
  await createStore(formData.get("name"))
  return { ok: true }
  // or: return redirect("/stores")  — triggers navigation after delete
}
```

Inline mutations (dialogs, forms that should not navigate away) use **`useFetcher`**. The fetcher exposes its own `state` and `data`, giving each dialog independent loading feedback:

```ts
const fetcher = useFetcher<typeof clientAction>()
// <fetcher.Form method="post"> ... </fetcher.Form>
// fetcher.state === "submitting" → show spinner
// fetcher.data?.error           → show error
```

Page-level manual refreshes use **`useRevalidator`** to re-run the loader on demand (the Refresh button on the stores list).

### API client (`app/lib/api.ts`)

All HTTP calls live in a single file. The base URL is `/api` (relative), which Quarkus resolves to its REST layer. Sharing an origin means no CORS configuration is needed in any environment.

The file exports typed async functions for every server endpoint (`listStores`, `createStore`, `deleteStore`, `queryFacts`, `createFactStream`) and two pure helpers:

- `decodePayload(data)` — base64-decodes the binary fact payload
- `relativeTime(isoString)` — formats a timestamp as a human-readable relative string (`"3m ago"`, `"just now"`, …)

### Live streaming

The stream page (`stores.$storeName.stream.tsx`) opens an `EventSource` to the SSE endpoint. `createFactStream()` in `api.ts` wraps the `EventSource` construction, wires `onmessage`/`onerror`, and returns the handle so the component can close it on unmount or reconnect.

The page implements a **freeze/resume** buffer: when frozen, incoming facts accumulate in a ref (`pendingRef`) rather than updating the display. On resume, the buffer flushes into the visible list. A `frozenRef` is kept in sync with the `frozen` state via `useEffect` — this avoids a stale closure inside the `EventSource` callback where reading `frozen` directly would always see the value captured at stream-start time.

### Key UI decisions

**Relative timestamps** are computed at render time and go stale. A `useTick(30_000)` hook in `FactTable` forces a re-render every 30 seconds so timestamps stay accurate without polling the server.

**Type badge colours** in `FactTable` use a deterministic string hash. The same fact type always gets the same colour regardless of render order or page state, and no module-level mutable cache is needed.

**Dark mode** uses an inline `<script>` injected into `<head>` before the page paints, reading `localStorage` and applying the `dark` class immediately. This prevents a flash of the wrong theme on load. The `ThemeToggle` component keeps `localStorage` and the `<html>` class in sync.

**Breadcrumbs** in `root.tsx` are derived entirely from `useLocation().pathname` — no context, no prop-drilling. They update on every navigation automatically.

### Route file naming

React Router v7 maps dot-separated file names to URL segments. The conventions used here:

| File | URL | Role |
|---|---|---|
| `stores.tsx` | `/stores` | Layout route — wraps the store list and its children |
| `stores.$storeName.tsx` | `/stores/:storeName` | Layout route — the per-store tab bar |
| `stores.$storeName._index.tsx` | `/stores/:storeName` (exact) | Index route — store overview |
| `stores.$storeName.facts.tsx` | `/stores/:storeName/facts` | Leaf route — fact browser |
| `stores.$storeName.stream.tsx` | `/stores/:storeName/stream` | Leaf route — live stream |

`$` denotes a dynamic URL segment (available as `params.storeName` in loaders and components). `_index` marks the index route that renders at the parent path when no child segment is matched.
