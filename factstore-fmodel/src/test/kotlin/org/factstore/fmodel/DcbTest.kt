package org.factstore.fmodel

import com.apple.foundationdb.FDB
import earth.adi.testcontainers.containers.FoundationDBContainer
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.factstore.avro.AvroFdbStore
import org.factstore.avro.FactRegistry
import org.factstore.avro.createAvroFactDescriptor
import org.factstore.core.AppendCondition
import org.factstore.core.FactId
import org.factstore.core.FactStore
import org.factstore.core.FactType
import org.factstore.core.TagKey
import org.factstore.core.TagQuery
import org.factstore.core.TagTypeItem
import org.factstore.core.TagValue
import org.factstore.foundationdb.buildFdbFactStore
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.*

const val FDB_VERSION = "7.3.69"
const val FDB_API_VERSION = 730

@TestInstance(PER_CLASS)
@Testcontainers
class DcbTest {

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
            val db = FDB.instance().open(clusterFilePath)
            store = buildFdbFactStore(
                clusterFilePath = testFdbCluster.clusterFilePath,
                name = "integration-test"
            )

            FactRegistry.register(createAvroFactDescriptor<ProjectAdded>("PROJECT_ADDED"))
        }

    }

    @Test
    fun testDcbImpl(): Unit = runBlocking {
        val avroStore = AvroFdbStore(store)
        val repo: DcbEventLockingRepository<TagQuery, ProjectAdded, FactId> = FdbDecisionModelRepo(avroStore)

        val addProjectDecider = addProjectDecider()
        val queryMapper = CommandToQueryMapper<AddProject, TagQuery> {
            TagQuery(
                queryItems = listOf(
                    TagTypeItem(
                        listOf(FactType("PROJECT_ADDED")),
                        tags = listOf(TagKey("projectId") to TagValue(projectId.toString()))
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

            val readEvents = avroStore.readSubject("PROJECT", command.projectId.toString())
            assertThat(readEvents).hasOnlyElementsOfType(ProjectAdded::class.java).hasSize(1)
        }

    }

}

class FdbDecisionModelRepo(
    private val store: AvroFdbStore
) : DcbEventLockingRepository<TagQuery, ProjectAdded, FactId> {

    override suspend fun TagQuery.fetchEvents(): Pair<Flow<ProjectAdded>, FactId?> {
        val events = store.readFromTagQuery(this)
        val lastId = events.lastOrNull()?.first
        return Pair(
            first = events.map { it.second }.filterIsInstance<ProjectAdded>().asFlow(),
            second = lastId
        )
    }

    override fun Flow<ProjectAdded>.save(
        query: TagQuery,
        version: FactId?
    ): Flow<ProjectAdded> = flow {
        val condition = AppendCondition.TagQueryBased(
            failIfEventsMatch = query,
            after = version
        )
        store.append(this@save.toList(), condition)
        emitAll(this@save)
    }
}