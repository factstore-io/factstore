import { NavLink, Outlet, useParams } from "react-router"
import { LayoutDashboard, Search, Radio } from "lucide-react"
import { cn } from "~/lib/utils"
import type { Route } from "./+types/stores.$storeName"

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.storeName} — FactStore Explorer` }]
}

const tabClass = ({ isActive }: { isActive: boolean }) =>
  cn(
    "flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors",
    isActive
      ? "border-foreground text-foreground"
      : "border-transparent text-muted-foreground hover:text-foreground hover:border-border",
  )

export default function StoreLayout() {
  const { storeName } = useParams<{ storeName: string }>()

  return (
    <div className="flex min-h-0 flex-col">
      <div className="border-b border-border bg-background">
        <div className="mx-auto max-w-6xl px-4">
          <nav className="flex gap-0.5 -mb-px" aria-label="Store navigation">
            <NavLink to={`/stores/${storeName}`} end className={tabClass}>
              <LayoutDashboard className="size-3.5" />
              Overview
            </NavLink>
            <NavLink to={`/stores/${storeName}/facts`} className={tabClass}>
              <Search className="size-3.5" />
              Facts
            </NavLink>
            <NavLink to={`/stores/${storeName}/stream`} className={tabClass}>
              <Radio className="size-3.5" />
              Live Stream
            </NavLink>
          </nav>
        </div>
      </div>

      <div className="flex-1 overflow-auto">
        <Outlet />
      </div>
    </div>
  )
}
