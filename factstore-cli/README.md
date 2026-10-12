# FactStore CLI

A fast, ergonomic command-line interface for [FactStore](https://github.com/factstore-io/factstore) — an append-only, ordered fact log built for production workloads.

---

## Installation

Build a native executable with:

```bash
./gradlew :factstore-cli:build -Dquarkus.native.enabled=true -Dquarkus.package.jar.enabled=false
```

Then create an alias for easy access:

```bash
alias factstore=$(pwd)/factstore-cli/build/factstore-cli-0.1.0-SNAPSHOT-runner
```

> **Tip:** Add the alias to your shell profile (`~/.zshrc`, `~/.bashrc`) to make it permanent.

---

## Quick Start

```bash
# Point the CLI at your FactStore instance
export FACTSTORE_URL=http://localhost:8080

# Create a store and append your first fact
factstore store create orders
factstore fact append '{"orderId": "12345", "amount": 100.0}' --store orders --subject order-12345 --type ORDER_PLACED

# Subscribe to facts in real time
factstore fact subscribe --store orders --from beginning
```

---

## Usage

### Store Management

```bash
# Create a new store
factstore store create orders

# List all stores
factstore store list

# Delete a store
factstore store remove orders
```

---

### Appending Facts

```bash
# Append a fact to a store
factstore fact append '{"orderId": "12345", "amount": 100.0}' \
  --store orders \
  --subject order/12345 \
  --type ORDER_PLACED

# Append with tags for richer querying
factstore fact append '{"orderId": "12345", "amount": 100.0}' \
  --store orders \
  --subject order/12345 \
  --type ORDER_PLACED \
  --tag region=eu \
  --tag env=prod
```

---

### Reading Facts

`fact get` reads a single fact; `fact stream` and `fact query` stream many, exactly as the
HTTP API does. Both streaming commands support `--limit`, `--direction`
(`forward` / `backward`, default: `forward`), `--continue-after` and `--output`.

Without `--limit` every matching fact is streamed; with it, at most that many, and the number
must be positive. All output formats print the facts as they arrive. The table has fixed
column widths and cuts longer values, so use `json` or `ndjson` to see a fact in full.

#### Get a fact by ID

```bash
factstore fact get 550e8400-e29b-41d4-a716-446655440000 --store orders
```

#### Stream facts

Without filters the whole store is streamed. `--subject`, `--type` and `--tag` are repeatable
and describe **one** criterion: the fact's subject must be one of the subjects, its type one of
the types, and it must carry **all** of the tags. The type is matched exactly, so
`com.acme.OrderPlaced` is not matched by `com.acme`:

```bash
# Everything in the store, newest first
factstore fact stream --store orders --direction backward --limit 20

# One entity's own facts
factstore fact stream --store orders --subject order/12345

# Every order ever placed
factstore fact stream --store orders --type com.acme.OrderPlaced

# Everything carrying both tags
factstore fact stream --store orders --tag region=eu --tag env=prod

# A combination: this type, for that region
factstore fact stream --store orders --type com.acme.OrderPlaced --tag region=eu
```

#### Continuing a stream

A stream stops at the head pinned when the command ran, so it always terminates. Pass the id of
the last fact you processed to `--continue-after` to pick up exactly where the previous run
stopped — it is exclusive and follows the reading order, so it works with `--direction backward`
too:

```bash
factstore fact stream --store orders --continue-after 550e8400-e29b-41d4-a716-446655440000
```

#### Query

A query asks several questions at once: a fact matches when it matches **any** of its filters.
Each filter is JSON; `--filter` adds one, and `--query-file` reads a whole query from a file:

```bash
factstore fact query --store orders \
  --filter '{"subjects":["order/42"]}' \
  --filter '{"types":["OrderPlaced"],"tags":{"region":"eu"}}'

factstore fact query --store orders --query-file query.json --direction backward
```

For a single filter `fact stream` is the simpler command.

#### Output formats

`--output` (`-o`) chooses how facts are printed: `table` (the default), `json` for one
pretty-printed array, or `ndjson` for one compact fact per line, as the HTTP API streams them.
Both JSON formats are written as the facts arrive, so they suit long results and pipelines:

```bash
factstore fact stream --store orders --type OrderPlaced -o ndjson | jq 'select(.subject == "order/42")'
```

---

### Subscribing to Facts

A subscription is `fact stream` without the stop: it catches up on the facts the store already
holds and then keeps printing the facts appended next, until you stop it with `Ctrl+C`. It
streams **every** fact of the store — subscriptions take no filters yet.

```bash
# Only new facts as they arrive (from the end)
factstore fact subscribe --store orders --from end

# Catch up from the beginning, then keep following
factstore fact subscribe --store orders --from beginning

# Resume where a stopped subscription left off
factstore fact subscribe --store orders --continue-after 550e8400-e29b-41d4-a716-446655440000

# As NDJSON for a pipeline, filtered on the reading side
factstore fact subscribe --store orders --from end -o ndjson | jq 'select(.type == "OrderPlaced")'
```

`--from` and `--continue-after` are alternatives: a subscription starts at one position.

---

### Environment Variables (TODO)

Avoid repeating common flags by setting environment variables:

| Variable | Flag equivalent | Description |
|---|---|---|
| `FACTSTORE_URL` | `--url` | FactStore server URL |
| `FACTSTORE_STORE` | `--store` | Default store name |

```bash
export FACTSTORE_URL=http://localhost:8080
export FACTSTORE_STORE=orders

# --store is no longer needed
factstore fact stream --type OrderPlaced
factstore fact subscribe
```

---

## Developer Notes

### Building

```bash
# Native executable (recommended for production use)
./gradlew :factstore-cli:build \
  -Dquarkus.native.enabled=true \
  -Dquarkus.package.jar.enabled=false

# JVM mode (faster build, useful during development)
./gradlew :factstore-cli:build
```

### Running in development

```bash
# Against a local FactStore instance
./factstore-cli/build/factstore-cli-0.1.0-SNAPSHOT-runner \
  --url http://localhost:8080 \
  store list
```
