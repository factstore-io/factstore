package com.cassisi.openeventstore.avro

import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.FactStore
import com.cassisi.openeventstore.core.FdbFactStoreResetHelper
import com.cassisi.openeventstore.core.impl.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AvroFactStoreTest {

    private lateinit var store: FactStore
    private lateinit var resetHelper: FdbFactStoreResetHelper

    @BeforeAll
    fun setupFDB() {
        FDB.selectAPIVersion(730)
        val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
        val fdbFactStore = FdbFactStore(db)
        store = FactStore(
            factAppender = FdbFactAppender(fdbFactStore),
            factFinder = FdbFactFinder(fdbFactStore),
            factStreamer = FdbFactStreamer(fdbFactStore),
            conditionalSubjectFactAppender = ConditionalFdbFactAppender(fdbFactStore),
            conditionalTagQueryFactAppender = ConditionalTagQueryFdbFactAppender(fdbFactStore)
        )
        resetHelper = FdbFactStoreResetHelper(fdbFactStore)
    }

    @BeforeEach
    fun clearEventStore() {
        resetHelper.reset()
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
