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
alias factstore=$(pwd)/factstore-cli/build/factstore-cli-1.0.0-SNAPSHOT-runner
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

### Querying Facts

Each query mode is a dedicated subcommand under `fact`. All commands support `--limit` (default: 100) and `--direction` (`forward` / `backward`, default: `forward`).

#### Find by ID

Direct lookup of a single fact by its UUID:

```bash
factstore fact find-by-id 550e8400-e29b-41d4-a716-446655440000 --store orders
```

#### Find by subject

```bash
factstore fact find-by-subject order-12345 --store orders
factstore fact find-by-subject order-12345 --store orders --limit 50 --direction backward
```

#### Find by tags

All specified tags must match (AND semantics):

```bash
factstore fact find-by-tags --store orders --tag region=eu
factstore fact find-by-tags --store orders --tag region=eu --tag env=prod
```

#### Find in time range

At least one of `--since` or `--until` is required:

```bash
# Facts in the last 5 minutes
factstore fact find-in-time-range --store orders --since 5m

# Facts in the last 2 hours, newest first
factstore fact find-in-time-range --store orders --since 2h --direction backward

# Absolute time range
factstore fact find-in-time-range --store orders \
  --since 2024-01-01T00:00:00Z \
  --until 2024-01-02T00:00:00Z
```

#### Time expressions

`--since` and `--until` accept both **relative durations** and **absolute ISO instants**:

| Expression | Meaning |
|---|---|
| `5m` | 5 minutes ago |
| `2h` | 2 hours ago |
| `1d` | 1 day ago |
| `2024-01-01T00:00:00Z` | Absolute timestamp |

---

### Subscribing to Facts

Subscribe to a store and stream facts in real time, similar to `tail -f`. A
subscription catches up on existing facts and then keeps emitting new ones; it runs
until you stop it.

```bash
# Only new facts as they arrive (from the end)
factstore fact subscribe --store orders --from end

# Catch up from the beginning, then keep following
factstore fact subscribe --store orders --from beginning

# Resume from a specific fact ID, then keep following
factstore fact subscribe --store orders --after 550e8400-e29b-41d4-a716-446655440000
```

Press `Ctrl+C` to stop. (`factstore fact stream` is kept as an alias for `subscribe`.)

---

### Replaying Facts

Replay drains existing facts **up to the current head and then exits** — ideal for
exports, projection rebuilds, and incremental batch jobs that need a terminating read
rather than a live tail. Facts appended while the replay runs are excluded.

```bash
# Replay everything currently in the store, then exit
factstore fact replay --store orders

# Incremental replay: only facts after a checkpoint, then exit
factstore fact replay --store orders --after 550e8400-e29b-41d4-a716-446655440000
```

Because every printed fact carries its id, a resumable batch job can persist the last
processed id and pass it to `--after` on the next run to continue exactly where it
left off.

---

### Output Formats (TODO)

All query and find commands support `--output` for machine-readable output:

```bash
# Default: human-readable table
factstore fact find-in-time-range --store orders --since 1h

# JSON output (pipe-friendly)
factstore fact find-in-time-range --store orders --since 1h --output json

# Pipe to jq
factstore fact find-in-time-range --store orders --since 1h --output json | jq '.[] | .type'
```

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
factstore fact find-in-time-range --since 5m
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
./factstore-cli/build/factstore-cli-1.0.0-SNAPSHOT-runner \
  --url http://localhost:8080 \
  store list
```
