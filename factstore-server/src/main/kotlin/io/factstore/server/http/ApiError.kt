package io.factstore.server.http

import io.factstore.core.FactId
import io.factstore.core.StoreName
import jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.CONFLICT
import jakarta.ws.rs.core.Response.Status.NOT_FOUND

data class ApiError(
    val status: String = "Failure",
    val message: String,
    val reason: Reason,
    val code: Int,
    val details: Map<String, Any?>? = null,
)

enum class Reason {
    NotFound,
    AlreadyExists,
    Conflict,
    ConditionViolated,
    InternalError,
}

fun ApiError.toResponse(): Response =
    Response
        .status(code)
        .entity(this)
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .build()


fun apiErrorResponse(
    status: Response.Status,
    reason: Reason,
    message: String,
    details: Map<String, Any?>? = null,
): Response =
    ApiError(
        status = "Failure",
        message = message,
        reason = reason,
        code = status.statusCode,
        details = details
    ).toResponse()

fun storeNotFoundError(storeName: StoreName): Response = apiErrorResponse(
    status = NOT_FOUND,
    reason = Reason.NotFound,
    message = "Store '$storeName' not found.",
    details = mapOf("kind" to "store", "name" to storeName.value)
)

fun factNotFoundError(factId: FactId): Response = apiErrorResponse(
    status = NOT_FOUND,
    reason = Reason.NotFound,
    message = "Fact '${factId.uuid}' not found.",
    details = mapOf("kind" to "fact", "id" to factId.uuid)
)

fun storeAlreadyExistsError(storeName: StoreName): Response = apiErrorResponse(
    status = CONFLICT,
    reason = Reason.AlreadyExists,
    message = "Store '$storeName' already exists.",
    details = mapOf("name" to storeName.value),
)

fun appendConditionViolatedError(): Response = apiErrorResponse(
    status = CONFLICT,
    reason = Reason.ConditionViolated,
    message = "The append condition was not satisfied.",
)

fun duplicateFactIdsError(factIds: List<FactId>): Response = apiErrorResponse(
    status = CONFLICT,
    reason = Reason.Conflict,
    message = "One or more fact IDs are already in use.",
    details = mapOf("factIds" to factIds.map { it.uuid }),
)
