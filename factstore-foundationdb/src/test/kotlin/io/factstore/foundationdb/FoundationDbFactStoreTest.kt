package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import io.factstore.core.FactFilter
import io.factstore.core.FactQuery
import io.factstore.core.ReadDirection
import io.factstore.core.StoreId
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import earth.adi.testcontainers.containers.FoundationDBContainer
import io.factstore.core.AppendResult
import io.factstore.core.CreateStoreRequest
import io.factstore.core.CreateStoreResult
import io.factstore.core.FactInput
import io.factstore.core.FactStore
import io.factstore.core.FactType
import io.factstore.core.RemoveStoreRequest
import io.factstore.core.RemoveStoreResult
import io.factstore.core.StoreName
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import io.factstore.core.toFactPayload
import io.factstore.testing.AbstractFactStoreTest
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
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

        private fun query(vararg filters: FactFilter) = FactQuery(filters.toList())

        private val region = TagKey("region") to TagValue("eu")
        private val tier = TagKey("tier") to TagValue("gold")

        @JvmStatic
        fun singleRangeQueries(): List<Arguments> = listOf(
            arguments("one subject", query(FactFilter(subjects = setOf(Subject("order/1")))), true),
            arguments("one type", query(FactFilter(types = setOf(FactType("T")))), true),
            arguments("one tag", query(FactFilter(tags = mapOf(region))), true),
            arguments("one type and one tag", query(FactFilter(types = setOf(FactType("T")), tags = mapOf(region))), true),
            arguments("two subjects", query(FactFilter(subjects = setOf(Subject("order/1"), Subject("order/2")))), false),
            arguments("two tags", query(FactFilter(tags = mapOf(region, tier))), false),
            arguments("a subject and a type", query(FactFilter(subjects = setOf(Subject("order/1")), types = setOf(FactType("T")))), false),
            arguments("one type and two tags", query(FactFilter(types = setOf(FactType("T")), tags = mapOf(region, tier))), false),
            arguments(
                "two filters of one subject each",
                query(FactFilter(subjects = setOf(Subject("order/1"))), FactFilter(subjects = setOf(Subject("order/2")))),
                false,
            ),
        )

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
    fun testRemovingStoreLeavesNoKeysBehind(): Unit = runBlocking {
        val storeName = StoreName("store-to-remove-completely")
        val storeId = (store.create(CreateStoreRequest(storeName)) as CreateStoreResult.Created).id
        val fact = FactInput(
            type = FactType("TEST_FACT_TYPE"),
            subject = Subject("TEST_SUBJECT"),
            payload = "DATA".toFactPayload(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("region") to TagValue("eu")),
        )
        assertThat(store.append(storeName, listOf(fact, fact))).isInstanceOf(AppendResult.Appended::class.java)

        // A store's keys are found by its id, which every one but the name index carries, and by
        // its name; looking only for those leaves whatever else the database holds out of it.
        val storeIdBytes = ByteBuffer.allocate(16)
            .putLong(storeId.uuid.mostSignificantBits)
            .putLong(storeId.uuid.leastSignificantBits)
            .array()
        val nameBytes = storeName.value.toByteArray()
        fun keysOfTheStore() = allKeys().filter { it.contains(storeIdBytes) || it.contains(nameBytes) }
        assertThat(keysOfTheStore()).isNotEmpty()

        assertThat(store.remove(RemoveStoreRequest(storeName))).isInstanceOf(RemoveStoreResult.StoreRemoved::class.java)

        // Facts included: a removed store is unreachable, so anything left behind would stay for good.
        assertThat(keysOfTheStore().map { ByteArrayUtil.printable(it) }).isEmpty()
    }

    private fun allKeys(): List<ByteArray> = db.read { tr ->
        tr.getRange(ByteArray(0), byteArrayOf(0xff.toByte())).asList().join().map { it.key }
    }

    /** Whether [part] occurs in these bytes. */
    private fun ByteArray.contains(part: ByteArray): Boolean =
        (0..size - part.size).any { start -> part.indices.all { this[start + it] == part[it] } }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singleRangeQueries")
    fun testQueryOfOneIndexRangeIsScanned(description: String, query: FactQuery, singleRange: Boolean) {
        val streamer = FdbFactStreamer(FdbFactStore(db, FdbFactStoreContext.create(Subspace(Tuple.from("test")))))
        val head = Versionstamp.complete(ByteArray(10))

        val source = with(streamer) { query.toSource(StoreId(UUID.randomUUID()), head, ReadDirection.Forward) }

        assertThat(source is PositionCursor).isEqualTo(singleRange)
    }

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
