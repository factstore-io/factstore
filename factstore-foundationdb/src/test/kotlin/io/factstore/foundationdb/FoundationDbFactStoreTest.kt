package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import earth.adi.testcontainers.containers.FoundationDBContainer
import io.factstore.core.FactStore
import io.factstore.testing.AbstractFactStoreTest
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

const val FDB_VERSION = "7.3.69"
const val FDB_API_VERSION = 730

@TestInstance(PER_CLASS)
@Testcontainers
class FactStoreTest : AbstractFactStoreTest() {

    companion object {

        lateinit var db: Database
        lateinit var store: FactStore

        @Container
        val testFdbCluster = FoundationDBContainer(DockerImageName.parse("foundationdb/foundationdb:$FDB_VERSION"))

        @JvmStatic
        @BeforeAll
        fun setupFDB() = runBlocking {
            FDB.selectAPIVersion(FDB_API_VERSION)
            db = FDB.instance().open(testFdbCluster.clusterFilePath)
            store = buildFdbFactStore(
                clusterFilePath = testFdbCluster.clusterFilePath,
            )
        }

    }


    override fun reset() {
        db.run { tr ->
            tr.clear(ByteArray(0), byteArrayOf(0xff.toByte()))
        }
    }

    override fun initializeFactStore(): FactStore = store

    @Test
    fun testMetadataRoundTripsWithNanos() {
        val metadata = FdbStoreMetadata(
            storeId = UUID.randomUUID(),
            name = "orders",
            createdAtEpochSeconds = 1_700_000_000L,
            createdAtNanos = 123_456_789,
        )

        assertThat(metadata.encode().toFdbStoreMetadata()).isEqualTo(metadata)
    }
}
