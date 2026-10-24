# FactStore CLI

A command-line interface for [FactStore](https://github.com/factstore-io/factstore) — an
append-only, ordered fact log built for production workloads. It is built on
[Clikt](https://ajalt.github.io/clikt/) and Quarkus, and ships as a native executable.

---

## Installation

Build the native executable. The build runs in Quarkus' builder container, so Docker or Podman
must be available, but no local GraalVM is needed:

```bash
./gradlew :factstore-cli:build \
  -Dquarkus.native.enabled=true \
  -Dquarkus.package.jar.enabled=false \
  -Dquarkus.native.container-build=true

alias factstore="$(echo $(pwd)/factstore-cli/build/factstore-cli-*-runner)"
```

During development the JVM build is faster to produce:

```bash
./gradlew :factstore-cli:build
alias factstore="java -jar $(pwd)/factstore-cli/build/quarkus-app/quarkus-run.jar"
```

The CLI is also published as a container image, which behaves like the binary:

```bash
docker run --rm ghcr.io/factstore-io/factstore-cli:main --help
```

---

## Quick start

```bash
export FACTSTORE_URL=http://localhost:8080

factstore store create orders
factstore fact append --store orders --type ORDER_PLACED --subject order/12345 \
  '{"orderId": "12345", "amount": 100.0}'

factstore fact subscribe --store orders --from beginning
```

---

## Which server, and which store

Both are resolved the way `kubectl` and `aws` resolve theirs: a flag wins, then the
environment, then the selected context in `~/.factstore/config`, then the default.

| | flag | environment | config file | default |
|---|---|---|---|---|
| server | `--url` | `FACTSTORE_URL` | the context's `url` | `http://localhost:8080` |
| context | `--context` | `FACTSTORE_CONTEXT` | `current-context` | none |
| store | `--store` / `-s` | `FACTSTORE_STORE` | the context's `store` | none (a usage error) |

`~/.factstore/config` holds named contexts, one of them current:

```properties
current-context = local

context.local.url = http://localhost:8080
context.local.store = demo-shop

context.prod.url = https://facts.internal:443
context.prod.store = orders
```

A missing file or key simply leaves that setting to the next source, but naming a context the
file does not define is an error: a mistyped `--context prod` must not quietly talk to
localhost. An `https` URL connects with TLS, and a URL without a port uses the scheme's own.

---

## Usage

### Stores

```bash
factstore store create orders
factstore store list               # or: store ls
factstore store list -o ndjson     # also: -o json
factstore store remove orders      # or: store rm
```

### Appending facts

```bash
factstore fact append --store orders --type ORDER_PLACED --subject order/12345 \
  --tag region=eu --tag env=prod \
  '{"orderId": "12345", "amount": 100.0}'

# Or read from stdin, by passing - as the payload
cat order.json | factstore fact append --store orders --type ORDER_PLACED --subject order/12345 -
```

A tag is `key=value`, and a fact carries one value per key. Without an idempotency key, retrying
an append after an ambiguous failure — a timeout, say — may append the fact twice. Pass the same
`--idempotency-key` (a UUID) on every attempt and the fact is appended at most once:

```bash
key=$(uuidgen)
factstore fact append --store orders --type ORDER_PLACED --subject order/1 --idempotency-key "$key" '{}'
factstore fact append --store orders --type ORDER_PLACED --subject order/1 --idempotency-key "$key" '{}'
# ℹ️ Append already applied; nothing was added.
```

### Reading facts

`fact get` reads a single fact; `fact stream` and `fact query` stream many, exactly as the HTTP
API does. Both streaming commands take `--limit`, `--direction` (`forward` / `backward`,
default `forward`), `--continue-after` and `--output`. Without `--limit` every matching fact is
streamed.

```bash
factstore fact get 550e8400-e29b-41d4-a716-446655440000 --store orders
```

#### Stream

Without options the whole store is streamed. `--subject`, `--type` and `--tag` are repeatable
and describe **one** query item: the fact's subject must be one of the subjects, its type one of
the types, and it must carry **all** of the tags. Types are matched exactly.

```bash
factstore fact stream --store orders --direction backward --limit 20
factstore fact stream --store orders --subject order/12345
factstore fact stream --store orders --type com.acme.OrderPlaced --tag region=eu
```

A stream stops at the head pinned when the command ran, so it always terminates. Pass the id of
the last fact you processed to `--continue-after` to pick up where a previous run stopped; it is
exclusive and follows the reading order, so it works backwards too.

#### Query

A query asks several questions at once: a fact matches when it matches **any** of its items.
`--item` adds one query item as JSON, and `--query-file` reads a whole query from a file:

```bash
factstore fact query --store orders \
  --item '{"subjects":["order/42"]}' \
  --item '{"types":["OrderPlaced"],"tags":{"region":"eu"}}'

factstore fact query --store orders --query-file query.json   # {"items":[…]}
```

Query items are read strictly, so a misspelled property is an error rather than an item that
silently matches more than was asked for. For a single item `fact stream` is simpler.

#### Output formats

`--output` (`-o`) chooses the shape: `table` (the default), `json` for one pretty-printed array,
or `ndjson` for one compact fact per line. Every shape is written as the facts arrive, so a long
history can be piped onwards while it is still being read:

```bash
factstore fact stream --store orders --type OrderPlaced -o ndjson | jq 'select(.subject == "order/42")'
```

The table's columns are sized to the terminal before the first row, not to the facts, so a fact
id is always shown in full and a long type, subject or payload is cut with `…`. Use `json` or
`ndjson` to see a fact whole.

### Subscribing

A subscription follows a store like `tail -f`: it prints facts as they are appended, until you
stop it with `Ctrl+C`. It starts at the store's end unless told otherwise, and follows **every**
fact of the store — subscriptions take no filters yet.

```bash
factstore fact subscribe --store orders                     # new facts only
factstore fact subscribe --store orders --from beginning    # replay, then follow
factstore fact subscribe --store orders --continue-after 550e8400-e29b-41d4-a716-446655440000
factstore fact subscribe --store orders -o ndjson | jq 'select(.type == "OrderPlaced")'
```

A subscription is written as `table` lines or as `ndjson`.

---

## Changes from the Picocli CLI

The CLI was rewritten on Clikt in place of Picocli. What a user of the previous one will notice:

- **`--url` and `FACTSTORE_URL` take effect.** Before, the gRPC channel was bound at start-up,
  before the flag was parsed.
- **`~/.factstore/config` holds contexts**, so the old flat `url` / `store` keys no longer apply.
- **`fact subscribe` starts at the store's end**, not its beginning. `--from beginning` replays.
- **A piped payload needs `-`**: `cat order.json | factstore fact append … -`. Without an
  argument the command no longer guesses whether stdin is a pipe.
- **`-t` is gone.** It used to mean `--type` for `fact append`; spell out `--type` or `--tag`,
  neither of which has a short form.
- **`store list` takes `-o`**, and `fact append` takes `--idempotency-key`.
- **`fact subscribe` no longer offers `-o json`**: a subscription never ends, so its facts can
  never form one array. Use `ndjson`.
- **A tag must be `key=value`**: `--tag region` is now an error rather than an empty value.
- **Exit codes**: every usage error exits 1, where Picocli used 2.

---

## Developer notes

### How it is put together

| Concern | Where |
|---|---|
| Process entry point, exit codes | `FactStoreCli.kt` — `@QuarkusMain`, the one `runBlocking` |
| Failure messages | `CliErrors.kt` — exhaustive `when` over the sealed `FactStoreException` |
| Which server, which store | `connection/` — resolution, and the clients that come out of it |
| What a command inherits | `command/ServerCommand.kt` — a `client`, and a `storeName` below it |
| Options more than one command takes | `command/CommonOptions.kt` — one extension function each |
| Command tree | `command/FactStoreCommand.kt` → `store/StoreCommand.kt`, `fact/FactCommand.kt` → the leaves |
| Writing facts and stores out | `output/` — pure functions, unit-tested without a server |

A command says what it accepts and calls the client library. It holds no connection code: it
extends `ServerCommand` (for `client`) or `StoreScopedCommand` (for `client` and `storeName`),
which is the only place that knows `ConnectionOptions` exists.

```kotlin
@Dependent
class CreateStoreCommand(clients: FactStoreClients) : ServerCommand(name = "create", clients = clients) {
    override fun help(context: Context) = "Create a new store"
    private val storeName by argument("name", help = "The unique name of the store")
    override suspend fun run() {
        val storeId = client.stores.create(storeName)
        echo("✅ Store '$storeName' created. Id: $storeId")
    }
}
```

Commands are `@Dependent` CDI beans, so each one names its collaborators in its constructor and
the parents wire their children with Clikt's `subcommands()`. There is no Quarkus extension for
Clikt, so `@QuarkusMain` + `QuarkusApplication` is the integration point: it parses with
`parse()` rather than Clikt's `main()`, because `main()` calls `exitProcess` and would cut
Quarkus' shutdown short.

Commands are `SuspendingCliktCommand`s, so client calls are plain `suspend` calls and the only
coroutine builder in the module is the one in `FactStoreCli`. The gRPC channel is built by
`FactStoreClients` from the resolved connection rather than injected with `@GrpcClient`: a
channel bound at start-up cannot see a flag parsed afterwards.

Validation happens while the command line is parsed wherever it can — tags, fact ids, query items,
contexts — so an error names the option that caused it. An error raised later, from `run()`,
goes through `usageError()`, which attaches the command's context so the right usage line is
printed with it.

### Output

A bounded read does not go through the terminal, in any shape. Output is written to a buffered
standard output, because Clikt's terminal is Mordant and Mordant reads every character on the
way past to style and size it: streaming 105k facts through it cost 1.2 s of a 2.9 s run. The
terminal is asked for its width, and nothing else.

That width settles the table's columns before the first row, which is what lets a table be
written a row at a time: columns sized to the facts would mean holding a whole history in
memory first. The whole row fits an 80-column terminal and no narrower one. A list of stores is
short and bounded, so it keeps Mordant's auto-sized table.

### Native executable

A CLI is a short-lived foreground command, so start-up time dominates and the CLI ships as a
native image. The binary links against the same glibc as the `ubi-minimal` base image it is
copied into by `src/main/docker/Dockerfile.native`, which is why the build runs in the builder
container rather than against a GraalVM installed on the host.

Two things a native image needed, both discovered by building one:

- **The connection is opened on gRPC's plain Netty transport**, not the shaded one the client
  library configures for itself (`Connection.openChannel`, and the `grpc-netty-shaded` exclusion
  in `build.gradle.kts`). Quarkus configures the plain transport for native images; the shaded
  copy has to be taught class by class, and `--initialize-at-build-time` on its package is no
  longer honoured.
- **The home directory comes from `$HOME`**, not from `user.home` (`FactStoreContexts`). GraalVM
  bakes that property while the image is built, which in a container build is the builder's
  home -- so every context in `~/.factstore/config` would be invisible, with no error to show
  for it.

### Mordant's terminal backend

Mordant, which renders the help and the tables, reaches the terminal through a native call, and
ships three backends that it tries in a fixed order: FFM, GraalVM's FFI, then JNA. `build.gradle.kts`
narrows that list per build mode, and the comment there explains why:

| build | backend | why the others are excluded |
|---|---|---|
| JVM | `mordant-jvm-ffm` | JNA calls a restricted method, which Java 25 warns about on every start |
| native | `mordant-jvm-graal-ffi` | the FFM backend loads inside a native image too, and is tried first |

That last exclusion is not cosmetic. With FFM on the native image's classpath, every command
fails on start-up with `MissingForeignRegistrationError: Cannot perform downcall …`, because
GraalVM wants FFM downcalls declared in `reachability-metadata.json`. The GraalVM FFI backend
exists precisely to avoid that, but it only gets its turn when FFM is absent.

Choosing the FFM backend is not enough on its own: it only loads for a module that is allowed
native access, and a CLI cannot ask its user to pass `--enable-native-access=ALL-UNNAMED`. So
the runner jar asks for it in its own manifest instead (`quarkus.package.jar.manifest.attributes`
in `application.yaml`), which JDK 24 and later honour for an executable jar. Without it Mordant
silently falls back to its dumb backend, which claims to be a terminal that is always 79 columns
wide: `factstore store list > stores.txt` would write escape sequences into the file, and a table
would never use the width it actually has.

### Still to decide

- Whether `factstore config use-context` / `get-contexts` are worth having yet. The config
  file's shape anticipates them.
- Whether `store remove` should confirm before deleting a store.
- Whether to teach `fact append` metadata.
