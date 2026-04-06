package io.factstore.memory

import io.factstore.core.FactStore
import io.factstore.testing.AbstractFactStoreTest
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS

@TestInstance(PER_CLASS)
class MemoryFactStoreTest : AbstractFactStoreTest() {

    override fun reset() {
        // No-op for in-memory store; we create a fresh instance in initializeFactStore()
    }

    override fun initializeFactStore(): FactStore = MemoryFactStore()

}

