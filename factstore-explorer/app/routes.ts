import { type RouteConfig, index, route } from "@react-router/dev/routes"

export default [
  index("routes/index.tsx"),
  route("stores", "routes/stores.tsx"),
  route("stores/:storeName", "routes/stores.$storeName.tsx", [
    index("routes/stores.$storeName._index.tsx"),
    route("facts", "routes/stores.$storeName.facts.tsx"),
    route("stream", "routes/stores.$storeName.stream.tsx"),
  ]),
] satisfies RouteConfig
