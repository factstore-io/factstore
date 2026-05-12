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

# Stream facts in real time
factstore fact stream orders --from beginning
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
factstore store delete orders
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

#### Direct lookup by ID

```bash
factstore find fact 550e8400-e29b-41d4-a716-446655440000 --store orders
```

#### Find facts with filters

All filters are optional and mutually exclusive. Results default to **oldest first** with a limit of **100**.

```bash
# Find facts for a specific subject
factstore find facts --store orders --subject order-12345

# Find facts in the last 5 minutes
factstore find facts --store orders --since 5m

# Find facts in the last 2 hours, newest first
factstore find facts --store orders --since 2h --direction backward

# Find facts in an absolute time range
factstore find facts --store orders \
  --since 2024-01-01T00:00:00Z \
  --until 2024-01-02T00:00:00Z

# Find facts by tag (AND semantics — all tags must match)
factstore find facts --store orders --tag region=eu
factstore find facts --store orders --tag region=eu --tag env=prod

# Control result size and order
factstore find facts --store orders --since 1h --limit 50 --direction backward
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

### Streaming Facts

Stream facts in real time, similar to `tail -f`:

```bash
# Stream new facts as they arrive (from the end)
factstore fact stream orders --from end

# Replay all facts from the beginning
factstore fact stream orders --from beginning

# Resume from a specific fact ID
factstore fact stream orders --after 550e8400-e29b-41d4-a716-446655440000
```

Press `Ctrl+C` to stop streaming.

---

### Output Formats (TODO)

All query and find commands support `--output` for machine-readable output:

```bash
# Default: human-readable table
factstore find facts --store orders --since 1h

# JSON output (pipe-friendly)
factstore find facts --store orders --since 1h --output json

# Pipe to jq
factstore find facts --store orders --since 1h --output json | jq '.[] | .type'
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
factstore find facts --since 5m
factstore fact stream
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
