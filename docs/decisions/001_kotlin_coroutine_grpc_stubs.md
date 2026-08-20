# 001 — Kotlin coroutine gRPC stubs, generated once in `factstore-proto`

**Date:** 2026-08-20
**Status:** Accepted

## Decision

This change uses Quarkus' new feature to use Kotlin coroutine gRPC stubs in FactStore Server,
which use the Vert.x context directly. No custom mapping between Mutiny and Kotlin is needed
anymore. This simplifies the code on the server side a lot.

Concretely, the three services now extend the generated `*CoroutineImplBase` classes and are
written as plain `suspend` functions and `Flow`s. `GrpcBridge.kt` — the `toUni` / `toMulti`
helpers that wrapped every service body — is gone, along with the manual `vertx.dispatcher()`
in each service. Quarkus discovers any bean extending `io.grpc.kotlin.AbstractCoroutineServerImpl`
and rewrites its `bindService()` to wrap the dispatcher in a context-preserving one, so
propagation is now the framework's job rather than ours.

However, there is still no way to let Quarkus generate the stubs. Its code generator invokes
protoc with a fixed set of outputs (`--java_out`, `--kotlin_out`, `--grpc_out`, `--q-grpc_out`,
`--descriptor_set_out`) and offers no extension point for a custom protoc plugin, so
`protoc-gen-grpc-kotlin` cannot be plugged in. For the time being, the stubs (client + server)
are generated once in the `factstore-proto` module. They are aligned with the protobuf and gRPC
versions from the Quarkus BOM.

## Why the versions have to be aligned

This part is not a matter of taste. Generated protobuf code calls
`RuntimeVersion.validateProtobufGencodeVersion(...)` in every message's static initializer, and
the runtime refuses to load gencode that is newer than itself:

> Detected incompatible Protobuf Gencode/Runtime versions […] Runtime version cannot be older
> than the linked gencode version.

Since `factstore-server` and `factstore-cli` apply the Quarkus BOM as an `enforcedPlatform`, the
BOM wins on their runtime classpath no matter what the rest of the build asks for. Generating
with a newer protoc than the BOM pins is therefore not a warning — it is a
`ProtobufRuntimeVersionException` at startup. So `factstore-proto` reads the protobuf and gRPC
versions straight out of the BOM instead of declaring its own, and there are no protobuf/gRPC
versions left in the version catalog. `grpc-kotlin` is the one exception: Quarkus has no
coroutine support, so its BOM does not manage it, and it is the only version this project picks
for itself.

## Two bits of wiring that are easy to delete by accident

- `quarkus.index-dependency.factstore-proto` in `application.yaml`. Quarkus identifies a
  coroutine service by walking its superclass chain up to `AbstractCoroutineServerImpl`, and it
  can only do that if the jar holding the generated base classes is in the Jandex index. Without
  this entry the services are silently skipped and the calls fail — the gRPC server still starts,
  which makes it a confusing failure.
- `io.quarkus:quarkus-vertx-kotlin` in the server dependencies. `quarkus-grpc` does not pull it
  in, but the rewritten `bindService()` references `ContextPreservingCoroutineDispatcher` from it.

## Trade-off

`factstore-proto` has become a coupling point for `factstore-server` and `factstore-client-kotlin`,
and uses the centrally managed Quarkus BOM versions to pull the corresponding dependencies for
stub generation. I accept the trade-off as it simplifies the integration for now, but we may want
to reconsider this once we have more clients or even dedicated repositories for clients.

Worth noting for whoever revisits this: letting each module generate its own stubs would not
actually decouple the versions today. `factstore-cli` is a Quarkus application that depends on
`factstore-client-kotlin`, so the client's generated code and the CLI's BOM-forced runtime end up
in the same JVM, and the gencode-vs-runtime rule above applies across that edge as well. The
coupling really dissolves when the client stops sharing a process with a Quarkus application —
which is exactly the "dedicated repositories for clients" scenario, and the right moment to split.
