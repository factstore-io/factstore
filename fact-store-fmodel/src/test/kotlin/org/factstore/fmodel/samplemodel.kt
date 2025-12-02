package org.factstore.fmodel

import com.fraktalio.fmodel.domain.Decider
import kotlinx.coroutines.flow.flowOf
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.factstore.avro.FactType
import org.factstore.avro.SubjectId
import org.factstore.avro.SubjectType
import java.util.UUID

data class AddProjectDecisionModel(
    val projectId: UUID
)


sealed interface Command

data class AddProject(
    val projectId: UUID,
    val projectName: String
) : Command



sealed interface Event

@Serializable
@FactType("PROJECT_ADDED")
@SubjectType("PROJECT")
data class ProjectAdded(
    @Contextual
    @SubjectId
    val projectId: UUID,
    val projectName: String
) : Event

typealias AddProjectDecider = Decider<AddProject, AddProjectDecisionModel?, ProjectAdded>

// decider impl

fun addProjectDecider() = AddProjectDecider(
    decide = { c, s ->
        when (c) {

            else -> {
                check(s == null) { "Project already exists" }
                flowOf(
                    ProjectAdded(
                        projectId = c.projectId,
                        projectName = c.projectName
                    )
                )
            }
        }
    },
    evolve = { _, e ->
        when (e) {
            else -> AddProjectDecisionModel(e.projectId)
        }
    },
    initialState = null
)
