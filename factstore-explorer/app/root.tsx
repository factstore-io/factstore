import {
  isRouteErrorResponse,
  Links,
  Meta,
  NavLink,
  Outlet,
  Scripts,
  ScrollRestoration,
  useLocation,
} from "react-router"
import { useEffect, useState } from "react"
import { Database, Moon, Sun, ChevronRight } from "lucide-react"
import { Button } from "~/components/ui/button"
import { cn } from "~/lib/utils"
import type { Route } from "./+types/root"
import "./app.css"

export const links: Route.LinksFunction = () => [
  { rel: "preconnect", href: "https://fonts.googleapis.com" },
  {
    rel: "preconnect",
    href: "https://fonts.gstatic.com",
    crossOrigin: "anonymous",
  },
  {
    rel: "stylesheet",
    href: "https://fonts.googleapis.com/css2?family=Inter:ital,opsz,wght@0,14..32,100..900;1,14..32,100..900&display=swap",
  },
]

function ThemeToggle() {
  const [dark, setDark] = useState(() => {
    if (typeof window === "undefined") return false
    return (
      localStorage.getItem("theme") === "dark" ||
      (!localStorage.getItem("theme") &&
        window.matchMedia("(prefers-color-scheme: dark)").matches)
    )
  })

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark)
    localStorage.setItem("theme", dark ? "dark" : "light")
  }, [dark])

  return (
    <Button
      variant="ghost"
      size="icon-sm"
      onClick={() => setDark((v) => !v)}
      aria-label="Toggle theme"
    >
      {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
    </Button>
  )
}

function useBreadcrumbs() {
  const location = useLocation()
  const segments = location.pathname.split("/").filter(Boolean)
  const crumbs: { label: string; to: string }[] = []

  if (segments.length === 0) {
    crumbs.push({ label: "Home", to: "/" })
  } else if (segments[0] === "stores") {
    crumbs.push({ label: "Stores", to: "/stores" })
    if (segments[1]) {
      crumbs.push({ label: segments[1], to: `/stores/${segments[1]}` })
      if (segments[2] === "facts") {
        crumbs.push({ label: "Facts", to: `/stores/${segments[1]}/facts` })
      } else if (segments[2] === "stream") {
        crumbs.push({ label: "Stream", to: `/stores/${segments[1]}/stream` })
      }
    }
  }

  return crumbs
}

function Breadcrumbs() {
  const crumbs = useBreadcrumbs()
  if (crumbs.length === 0) return null

  return (
    <nav className="flex items-center gap-1 text-sm" aria-label="Breadcrumb">
      {crumbs.map((crumb, i) => (
        <span key={crumb.to} className="flex items-center gap-1">
          {i > 0 && <ChevronRight className="size-3.5 text-muted-foreground" />}
          {i === crumbs.length - 1 ? (
            <span className="text-foreground font-medium">{crumb.label}</span>
          ) : (
            <NavLink
              to={crumb.to}
              className="text-muted-foreground hover:text-foreground transition-colors"
            >
              {crumb.label}
            </NavLink>
          )}
        </span>
      ))}
    </nav>
  )
}

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
        <script
          dangerouslySetInnerHTML={{
            __html: `(function(){var t=localStorage.getItem('theme');if(t==='dark'||(!t&&window.matchMedia('(prefers-color-scheme: dark)').matches)){document.documentElement.classList.add('dark')}})()`,
          }}
        />
      </head>
      <body className="bg-background text-foreground antialiased">
        {children}
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  )
}

export default function App() {
  return (
    <div className="flex min-h-screen flex-col">
      <header className="sticky top-0 z-40 border-b border-border bg-background/95 backdrop-blur-sm">
        <div className="flex h-12 items-center gap-4 px-4">
          <NavLink
            to="/"
            className="flex items-center gap-2 font-semibold text-sm shrink-0"
          >
            <Database className="size-4" />
            <span>FactStore Explorer</span>
          </NavLink>

          <div className="h-4 w-px bg-border shrink-0" />

          <div className="flex-1 min-w-0">
            <Breadcrumbs />
          </div>

          <div className="flex items-center gap-1 shrink-0">
            <ThemeToggle />
          </div>
        </div>
      </header>

      <main className="flex-1">
        <Outlet />
      </main>
    </div>
  )
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  let message = "Oops!"
  let details = "An unexpected error occurred."
  let stack: string | undefined

  if (isRouteErrorResponse(error)) {
    message = error.status === 404 ? "404" : "Error"
    details =
      error.status === 404
        ? "The requested page could not be found."
        : error.statusText || details
  } else if (import.meta.env.DEV && error && error instanceof Error) {
    details = error.message
    stack = error.stack
  }

  return (
    <div className="flex min-h-screen flex-col items-center justify-center gap-4 p-8">
      <h1 className="text-2xl font-bold">{message}</h1>
      <p className="text-muted-foreground">{details}</p>
      {stack && (
        <pre className="max-w-2xl overflow-x-auto rounded-md border border-border bg-muted p-4 text-xs">
          <code>{stack}</code>
        </pre>
      )}
    </div>
  )
}
