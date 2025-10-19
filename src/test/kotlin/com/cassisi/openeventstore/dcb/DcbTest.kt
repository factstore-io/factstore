package com.cassisi.openeventstore.dcb

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.avro.AvroFdbStore
import com.cassisi.openeventstore.avro.FactRegistry
import com.cassisi.openeventstore.avro.createAvroFactDescriptor
import com.cassisi.openeventstore.core.FactQueryItem
import com.cassisi.openeventstore.core.FactStore
import com.cassisi.openeventstore.core.FdbFactStoreResetHelper
import com.cassisi.openeventstore.core.TagQuery
import com.cassisi.openeventstore.core.TagQueryBasedAppendCondition
import com.cassisi.openeventstore.core.impl.ConditionalFdbFactAppender
import com.cassisi.openeventstore.core.impl.ConditionalTagQueryFdbFactAppender
import com.cassisi.openeventstore.core.impl.FdbFactAppender
import com.cassisi.openeventstore.core.impl.FdbFactFinder
import com.cassisi.openeventstore.core.impl.FdbFactStore
import com.cassisi.openeventstore.core.impl.FdbFactStreamer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import java.util.UUID

@TestInstance(PER_CLASS)
class DcbTest {

    private lateinit var store: FactStore
    private lateinit var resetHelper: FdbFactStoreResetHelper
    private lateinit var db: Database
    private lateinit var fdbFactStore: FdbFactStore

    @BeforeAll
    fun setupFDB() {
        FDB.selectAPIVersion(730)
        db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
        fdbFactStore = FdbFactStore(db)
        store = FactStore(
            factAppender = FdbFactAppender(fdbFactStore),
            factFinder = FdbFactFinder(fdbFactStore),
            factStreamer = FdbFactStreamer(fdbFactStore),
            conditionalSubjectFactAppender = ConditionalFdbFactAppender(fdbFactStore),
            conditionalTagQueryFactAppender = ConditionalTagQueryFdbFactAppender(fdbFactStore),
        )
        resetHelper = FdbFactStoreResetHelper(fdbFactStore)


        FactRegistry.register(createAvroFactDescriptor<ProjectAdded>("PROJECT_ADDED"))
    }

    @BeforeEach
    fun clearEventStore() {
        resetHelper.reset()
    }

    @Test
    fun testDcbImpl(): Unit = runBlocking {


        val avroStore = AvroFdbStore(store)
        val repo: DcbEventLockingRepository<TagQuery, ProjectAdded, UUID> = FdbDecisionModelRepo(avroStore)

        val addProjectDecider = addProjectDecider()
        val queryMapper = CommandToQueryMapper<AddProject, TagQuery> {
            TagQuery(
                queryItems = listOf(
                    FactQueryItem(
                        listOf("PROJECT_ADDED"),
                        tags = listOf("projectId" to projectId.toString())
                    )
                )
            )
        }

        val decisionModel = EventSourcingLockingDecisionModel(
            decider = addProjectDecider,
            mapper = queryMapper,
            eventRepository = repo
        )


        repeat(10) {
            val command = AddProject(
                projectId = UUID.randomUUID(),
                projectName = "Test123"
            )

            val newEvents = decisionModel.handleOptimistically(command).toList()
            assertThat(newEvents).isNotEmpty()
        }


        avroStore.readFromTagQuery(TagQuery(queryItems = listOf(
            FactQueryItem(
                types = listOf("PROJECT_ADDED"),
                tags = listOf()
            )
        ))).forEach { println(it) }

    }

}

class FdbDecisionModelRepo(
    private val store: AvroFdbStore
) : DcbEventLockingRepository<TagQuery, ProjectAdded, UUID> {

    override suspend fun TagQuery.fetchEvents(): Pair<Flow<ProjectAdded>, UUID?> {
        val events = store.readFromTagQuery(this)
        val lastId = events.lastOrNull()?.first
        return Pair(
            first = events.map { it.second }.filterIsInstance<ProjectAdded>().asFlow(),
            second = lastId
        )
    }

    override fun Flow<ProjectAdded>.save(
        query: TagQuery,
        version: UUID?
    ): Flow<ProjectAdded> = flow {
        val condition = TagQueryBasedAppendCondition(
            failIfEventsMatch = query,
            after = version
        )
        store.append(this@save.toList(), condition)
        emitAll(this@save)
    }
}