package io.factstore.cli.client

import io.factstore.client.model.Fact
import io.quarkus.runtime.annotations.RegisterForReflection

@RegisterForReflection(
    targets= [
        Fact::class,
    ]
)
class FactStoreReflectionConfiguration
