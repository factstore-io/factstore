package io.factstore.foundationdb

import com.apple.foundationdb.FDB
import earth.adi.testcontainers.containers.FoundationDBContainer
import io.factstore.core.*
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@TestInstance(PER_CLASS)
@Testcontainers
class FactStoreFactoryTest {

    companion object {

        lateinit var factory: FactStoreFactory
        lateinit var finder: FactStoreFinder
        lateinit var resetHelper: FdbFactStoreResetHelper
        lateinit var clusterFilePath: String

        @Container
        val testFdbCluster = FoundationDBContainer(DockerImageName.parse("foundationdb/foundationdb:$FDB_VERSION"))

        @JvmStatic
        @BeforeAll
        fun setupFDB() = runBlocking {
            FDB.selectAPIVersion(FDB_API_VERSION)
            clusterFilePath = testFdbCluster.clusterFilePath
            val db = FDB.instance().open(clusterFilePath)
            val context = FoundationDBFactStoreContext(db)
            factory = FdbFactStoreFactory(context)
            finder = FdbFactStoreFinder(context)
            resetHelper = FdbFactStoreResetHelper(db)
        }

    }

    @BeforeEach
    fun clearEventStore() {
        resetHelper.reset()
    }

    @Test
    fun testCreateFactStore(): Unit = runBlocking {
        val name = FactStoreName("test")
        val request = CreateFactStoreRequest(name)

        val result = factory.handle(request)

        assertThat(result)
            .isNotNull()
            .isInstanceOf(CreateFactStoreResult.Created::class.java)

        // creating another fact store with the same name should be rejected
        val secondResult = factory.handle(request)

        assertThat(secondResult)
            .isNotNull()
            .isInstanceOf(CreateFactStoreResult.NameAlreadyExists::class.java)

        // creating another fact store with a different name should work

        val anotherName = FactStoreName("another-store")
        val anotherRequest = CreateFactStoreRequest(anotherName)

        val thirdResult = factory.handle(anotherRequest)

        assertThat(thirdResult)
            .isNotNull()
            .isInstanceOf(CreateFactStoreResult.Created::class.java)

        assertThat(finder.existsByName(name)).isTrue()
        assertThat(finder.existsByName(anotherName)).isTrue()
        assertThat(finder.existsByName(FactStoreName("non-existing"))).isFalse()

        assertThat(finder.listAll()).size().isEqualTo(2)
    }

}
