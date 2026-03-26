package io.factstore.foundationdb

import earth.adi.testcontainers.containers.FoundationDBContainer
import kotlinx.coroutines.*
import org.assertj.core.api.Assertions.*
import io.factstore.core.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import java.util.Collections

@TestInstance(PER_CLASS)
@Testcontainers
class FdbFactStoreFactoryTest {

    companion object {
        @Container
        val testFdbCluster = FoundationDBContainer(
            DockerImageName.parse("foundationdb/foundationdb:$FDB_VERSION")
        )

        lateinit var factory: FdbFactStoreFactory
        lateinit var resetHelper: FdbFactStoreResetHelper
        lateinit var fdbContext: FoundationDBFactStoreContext

        @JvmStatic
        @BeforeAll
        fun setupFDB() = runBlocking {
            println("setup start")
            fdbContext = initDatabase(testFdbCluster.clusterFilePath, FDB_API_VERSION)
            resetHelper = FdbFactStoreResetHelper(fdbContext.database)
            println("setup end")
        }
    }

    @BeforeEach
    fun resetDatabase() = runBlocking {
        resetHelper.reset()
        factory = FdbFactStoreFactory.create(fdbContext)
    }

    @Test
    fun `create should create a fact store with valid name`(): Unit = runBlocking {
        val metadata = factory.create("my-store")

        assertThat(metadata).isNotNull
        assertThat(metadata.name).isEqualTo("my-store")
        assertThat(metadata.id).isNotNull
        assertThat(metadata.createdAt).isGreaterThan(0)
    }

    @Test
    fun `create should reject duplicate names`(): Unit = runBlocking {
        factory.create("duplicate-test")

        val exception = catchThrowable {
            runBlocking { factory.create("duplicate-test") }
        }

        assertThat(exception)
            .isInstanceOf(FactStoreAlreadyExistsException::class.java)
            .hasMessageContaining("duplicate-test")
    }

    @Test
    fun `create should reject empty names`(): Unit = runBlocking {
        val exception = catchThrowable {
            runBlocking { factory.create("") }
        }

        assertThat(exception)
            .isInstanceOf(InvalidFactStoreNameException::class.java)
            .hasMessageContaining("empty")
    }

    @Test
    fun `create should reject names with invalid characters`(): Unit = runBlocking {
        listOf(
            "my store", // space
            "my.store", // dot
            "my/store", // slash
            "my@store", // at sign
            "my:store", // colon
            "my\$store", // dollar
            "Mÿ_store", // non-ASCII
        ).forEach { invalidName ->
            val exception = catchThrowable {
                runBlocking { factory.create(invalidName) }
            }

            assertThat(exception)
                .isInstanceOf(InvalidFactStoreNameException::class.java)
        }
    }

    @Test
    fun `create should accept alphanumeric and hyphen and underscore`(): Unit = runBlocking {
        val validNames = listOf(
            "my-store",
            "my_store",
            "MyStore123",
            "store-123",
            "store_456",
            "store123",
            "a",
            "A",
            "0",
            "abc123_-def",
        )

        validNames.forEach { validName ->
            val metadata = factory.create(validName)
            assertThat(metadata.name).isEqualTo(validName)
        }
    }

    @Test
    fun `create should reject names exceeding 255 characters`(): Unit = runBlocking {
        val longName = "a".repeat(256)

        val exception = catchThrowable {
            runBlocking { factory.create(longName) }
        }

        assertThat(exception)
            .isInstanceOf(InvalidFactStoreNameException::class.java)
            .hasMessageContaining("255")
    }

    @Test
    fun `get should retrieve an existing fact store`(): Unit = runBlocking {
        factory.create("test-store")

        val factStore = factory.get("test-store")

        assertThat(factStore).isNotNull
        assertThat(factStore).isInstanceOf(FactStore::class.java)
    }

    @Test
    fun `get should throw for non-existent fact store`(): Unit = runBlocking {
        val exception = catchThrowable {
            runBlocking { factory.get("non-existent") }
        }

        assertThat(exception)
            .isInstanceOf(FactStoreNotFoundException::class.java)
            .hasMessageContaining("non-existent")
    }

    @Test
    fun `getMetadata should retrieve metadata for existing fact store`(): Unit = runBlocking {
        val created = factory.create("metadata-test")

        val retrieved = factory.getMetadata("metadata-test")

        assertThat(retrieved.name).isEqualTo(created.name)
        assertThat(retrieved.id).isEqualTo(created.id)
        assertThat(retrieved.createdAt).isEqualTo(created.createdAt)
    }

    @Test
    fun `getMetadata should throw for non-existent fact store`(): Unit = runBlocking {
        val exception = catchThrowable {
            runBlocking { factory.getMetadata("non-existent") }
        }

        assertThat(exception)
            .isInstanceOf(FactStoreNotFoundException::class.java)
    }

    @Test
    fun `exists should return true for existing fact store`(): Unit = runBlocking {
        factory.create("exists-test")

        val exists = factory.exists("exists-test")

        assertThat(exists).isTrue
    }

    @Test
    fun `exists should return false for non-existent fact store`(): Unit = runBlocking {
        val exists = factory.exists("non-existent")

        assertThat(exists).isFalse
    }

    @Test
    fun `delete should remove a fact store`(): Unit = runBlocking {
        factory.create("delete-test")

        factory.delete("delete-test")

        val exists = factory.exists("delete-test")
        assertThat(exists).isFalse
    }

    @Test
    fun `delete should throw for non-existent fact store`(): Unit = runBlocking {
        val exception = catchThrowable {
            runBlocking { factory.delete("non-existent") }
        }

        assertThat(exception)
            .isInstanceOf(FactStoreNotFoundException::class.java)
    }

    @Test
    fun `delete should make name available for reuse`(): Unit = runBlocking {
        factory.create("reusable-name")
        factory.delete("reusable-name")

        // Should not throw
        val metadata = factory.create("reusable-name")
        assertThat(metadata.name).isEqualTo("reusable-name")
    }

    @Test
    fun `delete should cascade delete all facts`(): Unit = runBlocking {
        factory.create("cascade-delete-test")
        val store = factory.get("cascade-delete-test")

        // Append facts
        store.append(
            Fact(
                id = FactId.generate(),
                type = "created".toFactType(),
                payload = "{}".toFactPayload(),
                subjectRef = SubjectRef("entity1", "entity1"),
                appendedAt = Instant.now()
            )
        )
        store.append(
            Fact(
                id = FactId.generate(),
                type = "updated".toFactType(),
                payload = "{}".toFactPayload(),
                subjectRef = SubjectRef("entity2", "entity2"),
                appendedAt = Instant.now()
            )
        )

        // Delete the store
        factory.delete("cascade-delete-test")

        // Recreate with same name
        factory.create("cascade-delete-test")
        val newStore = factory.get("cascade-delete-test")

        // New store should be empty
        val found = newStore.findInTimeRange(Instant.EPOCH, Instant.now())
        assertThat(found).isEmpty()
    }

    @Test
    fun `listAll should return all fact stores`(): Unit = runBlocking {
        val created1 = factory.create("store1")
        val created2 = factory.create("store2")
        val created3 = factory.create("store3")

        val all = factory.listAll()

        assertThat(all).hasSize(3)
        assertThat(all.map { it.name }).containsExactlyInAnyOrder("store1", "store2", "store3")
        assertThat(all.map { it.id }).containsExactly(created1.id, created2.id, created3.id)
    }

    @Test
    fun `listAll should return empty list when no fact stores exist`(): Unit = runBlocking {
        val all = factory.listAll()

        assertThat(all).isEmpty()
    }

    @Test
    fun `concurrent creates with same name should fail exactly once`(): Unit = runBlocking {
        val exceptions = Collections.synchronizedList(mutableListOf<Throwable>())
        val coroutines = (1..10).map {
            launch {
                try {
                    factory.create("concurrent-test")
                } catch (e: FactStoreAlreadyExistsException) {
                    exceptions.add(e)
                }
            }
        }

        coroutines.joinAll()

        // Exactly 9 should fail (1 succeeds, 9 fail)
        assertThat(exceptions).hasSize(9)
    }

    @Test
    fun `concurrent mixed operations should maintain consistency`(): Unit = runBlocking {
        // Create initial stores
        factory.create("store1")
        factory.create("store2")

        val results = Collections.synchronizedList(mutableListOf<String>())

        val jobs = mutableListOf<Job>()

        // Create new stores
        for (i in 3..5) {
            jobs.add(launch {
                try {
                    factory.create("store$i")
                    results.add("created_store$i")
                } catch (@Suppress("UNUSED_VARIABLE") e: Exception) {
                    results.add("failed_store$i")
                }
            })
        }

        // Check existence
        for (i in 1..5) {
            jobs.add(launch {
                val exists = factory.exists("store$i")
                results.add("exists_store$i:$exists")
            })
        }

        // Get metadata
        for (i in 1..3) {
            jobs.add(launch {
                try {
                    factory.getMetadata("store$i")
                    results.add("got_metadata_store$i")
                } catch (@Suppress("UNUSED_VARIABLE") e: Exception) {
                    results.add("failed_metadata_store$i")
                }
            })
        }

        jobs.joinAll()

        // Verify all operations completed
        assertThat(results).hasSize(11) // 3 creates + 5 exists checks + 3 metadata checks
        
        // Verify final state
        val finalList = factory.listAll()
        assertThat(finalList).hasSize(5)
    }

    @Test
    fun `each fact store should have unique UUID`(): Unit = runBlocking {
        val meta1 = factory.create("unique1")
        val meta2 = factory.create("unique2")
        val meta3 = factory.create("unique3")

        assertThat(meta1.id)
            .isNotEqualTo(meta2.id)
            .isNotEqualTo(meta3.id)
        assertThat(meta2.id).isNotEqualTo(meta3.id)
    }

    @Test
    fun `metadata should have monotonically increasing createdAt timestamps`(): Unit = runBlocking {
        val meta1 = factory.create("time1")
        val meta2 = factory.create("time2")
        val meta3 = factory.create("time3")

        assertThat(meta1.createdAt)
            .isLessThanOrEqualTo(meta2.createdAt)
            .isLessThanOrEqualTo(meta3.createdAt)
    }
}




