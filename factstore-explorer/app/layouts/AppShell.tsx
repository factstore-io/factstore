import { Outlet, useLocation } from "react-router";
import { useState } from "react";
import type { Store } from "~/types";
import { INITIAL_STORES } from "~/lib/mockData";
import Sidebar from "~/components/Sidebar";
import AppBar from "~/components/AppBar";

export type AppShellContext = {
  stores: Store[];
  setStores: React.Dispatch<React.SetStateAction<Store[]>>;
  currentStore: Store | undefined;
};

export default function AppShell() {
  const [stores, setStores] = useState<Store[]>(INITIAL_STORES);
  const location = useLocation();

  const storeMatch = location.pathname.match(/^\/stores\/([^/]+)\//);
  const storeId = storeMatch?.[1];
  const currentStore = storeId ? stores.find((s) => s.id === storeId) : undefined;

  const context: AppShellContext = { stores, setStores, currentStore };

  return (
    <div className="app-shell">
      <Sidebar currentStore={currentStore} />
      <div className="main-wrapper">
        <AppBar currentStore={currentStore} />
        <main className="main-content">
          <Outlet context={context} />
        </main>
      </div>
    </div>
  );
}
