import {
  type RouteConfig,
  index,
  layout,
  route,
} from "@react-router/dev/routes";

export default [
  layout("layouts/AppShell.tsx", [
    index("routes/stores.tsx"),
    route("stores/:storeId/facts", "routes/facts.tsx"),
    route("stores/:storeId/stream", "routes/stream.tsx"),
  ]),
] satisfies RouteConfig;
