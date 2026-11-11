package io.factstore.storage.memory

import io.factstore.storage.FactLogContractTest

class InMemoryFactLogTest : FactLogContractTest() {

    override fun createLog() = InMemoryFactLog()
}
