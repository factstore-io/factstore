package org.factstore.avro

import com.apple.foundationdb.FDB
import earth.adi.testcontainers.containers.FoundationDBContainer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.factstore.core.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import java.util.*

const val FDB_VERSION = "7.3.69"
const val FDB_API_VERSION = 730

@TestInstance(PER_CLASS)
@Testcontainers
class AvroFactStoreTest {

    companion object {

        lateinit var store: FactStore
        lateinit var clusterFilePath: String

        @Container
        val testFdbCluster = FoundationDBContainer(DockerImageName.parse("foundationdb/foundationdb:$FDB_VERSION"))

        @JvmStatic
        @BeforeAll
        fun setupFDB() {
            FDB.selectAPIVersion(FDB_API_VERSION)
            clusterFilePath = testFdbCluster.clusterFilePath
            store = buildFdbFactStore(
                clusterFilePath = testFdbCluster.clusterFilePath,
                name = "integration-test"
            )
        }

    }

    @Test
    fun testAvroFdbStore(): Unit = runBlocking {

        val avroStore = AvroFdbStore(store)

        // TODO: avoid passing the type again here, rather extract from annotations!
        FactRegistry.register(createAvroFactDescriptor<UserOnboarded>("USER_ONBOARDED"))
        FactRegistry.register(createAvroFactDescriptor<UsernameChanged>("USERNAME_CHANGED"))


        val userId = UUID.randomUUID()

        avroStore.append(
            UserOnboarded(
                userId = userId,
                username = "domenic",
                onboardedAt = Instant.now()
            )
        )

        avroStore.append(
            UsernameChanged(
                userId = userId,
                username = "domenic2",
                onboardedAt = Instant.now()
            )
        )

        avroStore.readSubject("USER", userId.toString()).forEach {
            println("${it::class.simpleName} $it")
        }
    }

}


@Serializable
@FactType("USER_ONBOARDED")
@SubjectType("USER")
data class UserOnboarded(
    @SerialName("userId")
    @Contextual
    @SubjectId
    val userId: UUID,

    @SerialName("username")
    @Tag("username")
    val username: String,

    @Contextual
    @SerialName("onboardedAt")
    val onboardedAt: Instant,
)


@Serializable
@FactType("USERNAME_CHANGED")
@SubjectType("USER")
data class UsernameChanged(
    @SerialName("userId")
    @Contextual
    @SubjectId
    val userId: UUID,

    @SerialName("username")
    @Tag("username")
    val username: String,

    @Contextual
    @SerialName("onboardedAt")
    val onboardedAt: Instant,
)
