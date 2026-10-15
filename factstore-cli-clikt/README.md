# factstore-cli-clikt

The FactStore CLI, rebuilt on [Clikt](https://ajalt.github.io/clikt/) instead of Picocli.

This module exists next to `factstore-cli` so the migration can proceed without touching the
CLI that ships today. Every command of that CLI is covered:

```bash
./gradlew :factstore-cli-clikt:build
alias factstore="java -jar $(pwd)/factstore-cli-clikt/build/quarkus-app/quarkus-run.jar"

factstore store create orders          # also: store list (ls), store remove (rm)
factstore fact append --type OrderPlaced --subject order/1 '{"total":5}'
factstore fact stream --store orders --tag region=eu
factstore fact query --store orders --filter '{"types":["OrderPlaced"]}'
factstore fact subscribe --store orders
factstore fact get <factId> --store orders -o json
```

## Which server, and which store

Both are resolved the way `kubectl` and `aws` resolve theirs, and in one place only
([`ConnectionOptions`](src/main/kotlin/io/factstore/cli/connection/ConnectionOptions.kt)):

| | flag | environment | config file | default |
|---|---|---|---|---|
| server | `--url` | `FACTSTORE_URL` | the context's `url` | `http://localhost:8080` |
| context | `--context` | `FACTSTORE_CONTEXT` | `current-context` | none |
| store | `--store` / `-s` | `FACTSTORE_STORE` | the context's `store` | none (a usage error) |

`~/.factstore/config` holds the contexts:

```properties
current-context = local

context.local.url = http://localhost:8080
context.local.store = demo-shop

context.prod.url = https://facts.internal:443
context.prod.store = orders
```

`factstore config use-context` and friends are deliberately not built yet: the file shape
anticipates them, and they are worth writing once there is a second environment to switch
between.

## How it is put together

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
channel bound at startup cannot see a flag parsed afterwards, which is why `--url` never worked
in the Picocli CLI.

## Native executable

A CLI is a short-lived foreground command, so start-up time dominates: the shipping CLI is a
native image, and this one builds the same way. There is no local GraalVM requirement, because
the build runs in Quarkus' builder container (Docker or Podman must be available):

```bash
./gradlew :factstore-cli-clikt:build \
  -Dquarkus.native.enabled=true \
  -Dquarkus.package.jar.enabled=false \
  -Dquarkus.native.container-build=true

./factstore-cli-clikt/build/factstore-cli-clikt-*-runner store ls
```

The binary links against the same glibc as the `ubi-minimal` base image it is copied into by
`src/main/docker/Dockerfile.native`, which is why the container build is used rather than a
GraalVM installed on the host.

### What a native image needed

Two things, both discovered by building one:

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

## Deliberate differences from the Picocli CLI

- **`fact subscribe` starts at the store's end**, not its beginning: the command promises
  `tail -f`, and `tail -f` does not print the whole file first. `--from beginning` still
  replays. The specification's own default stays `Beginning`, for projections.
- **`-t` is `--tag` everywhere.** In the Picocli CLI `-t` meant `--type` for `fact append` and
  `--tag` for `fact stream`; `--type` now has no short form.
- **Machine-readable output does not go through the terminal.** `json` and `ndjson` are written
  to a buffered standard output, because Clikt's terminal is Mordant and Mordant reads every
  character on the way past to style and size it: streaming 105k facts through it cost 1.2 s of
  a 2.9 s run. Tables still go through the terminal, which is what needs its width and theme.
- **Tables are Mordant's**, so columns size themselves. A table is rendered to the width
  Mordant reports, which falls back to 79 columns when it cannot see a terminal — narrow enough
  to cut a fact's id, which is what `-o ndjson` is for in a pipeline (`COLUMNS` widens it on the
  way to a pager).
- **Exit codes**: every `CliktError` exits 1, where Picocli used 2 for usage errors. Re-assert 2
  if anything depends on it.
- **`~/.factstore/config` is read as contexts**, so the old flat `url` / `store` keys no longer
  apply.

## Still to decide

- Whether `factstore config use-context` / `get-contexts` are worth having yet.
- Whether `store remove` should confirm before deleting a store; neither CLI does today.
- Whether to teach `fact append` metadata, which neither CLI supports.
- When to retire `factstore-cli` and point `container.yml` at this module.
