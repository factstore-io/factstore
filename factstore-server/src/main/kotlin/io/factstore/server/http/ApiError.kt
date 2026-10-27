package io.factstore.server.http

import io.factstore.core.FactId
import io.factstore.core.IdempotencyKey
import io.factstore.core.StoreName
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.CONFLICT
import jakarta.ws.rs.core.Response.Status.NOT_FOUND

data class ApiError(
    val status: String = "Failure",
    val message: String,
    val reason: Reason,
    val code: Int,
    val details: Map<String, String> = emptyMap(),
)

enum class Reason {
    InvalidInput,
    NotFound,
    AlreadyExists,
    ConditionViolated,
    IdempotencyKeyReused,
    InternalError,
}

internal fun ApiError.toResponse(): Response =
    Response
        .status(code)
        .entity(this)
        .type(APPLICATION_JSON)
        .build()

internal fun apiErrorResponse(
    status: Response.StatusType,
    reason: Reason,
    message: String,
    details: Map<String, String> = emptyMap(),
): Response =
    ApiError(
        message = message,
        reason = reason,
        code = status.statusCode,
        details = details,
    ).toResponse()

internal fun storeNotFoundError(storeName: StoreName): Response = apiErrorResponse(
    status = NOT_FOUND,
    reason = Reason.NotFound,
    message = "Store '$storeName' not found.",
    details = mapOf("kind" to "store", "name" to storeName.value)
)

internal fun factNotFoundError(factId: FactId): Response = apiErrorResponse(
    status = NOT_FOUND,
    reason = Reason.NotFound,
    message = "Fact '${factId.uuid}' not found.",
    details = mapOf("kind" to "fact", "id" to factId.uuid.toString())
)

internal fun continuationNotFoundError(factId: FactId): Response = apiErrorResponse(
    status = NOT_FOUND,
    reason = Reason.NotFound,
    message = "The fact '${factId.uuid}' to continue after does not exist.",
    details = mapOf("kind" to "continuation", "id" to factId.uuid.toString())
)

internal fun afterFactNotFoundError(factId: FactId): Response = apiErrorResponse(
    status = NOT_FOUND,
    reason = Reason.NotFound,
    message = "The fact '${factId.uuid}' the append condition checks after does not exist.",
    details = mapOf("kind" to "afterFact", "id" to factId.uuid.toString())
)

internal fun storeAlreadyExistsError(storeName: StoreName): Response = apiErrorResponse(
    status = CONFLICT,
    reason = Reason.AlreadyExists,
    message = "Store '$storeName' already exists.",
    details = mapOf("name" to storeName.value),
)

internal fun appendConditionViolatedError(): Response = apiErrorResponse(
    status = CONFLICT,
    reason = Reason.ConditionViolated,
    message = "The append condition was not satisfied.",
)


internal fun idempotencyKeyReusedError(idempotencyKey: IdempotencyKey): Response = apiErrorResponse(
    status = UnprocessableContent,
    reason = Reason.IdempotencyKeyReused,
    message = "The idempotency key '${idempotencyKey.uuid}' was used by a different request.",
    details = mapOf("idempotencyKey" to idempotencyKey.uuid.toString()),
)

private object UnprocessableContent : Response.StatusType {
    override fun getStatusCode(): Int = 422
    override fun getFamily(): Response.Status.Family = Response.Status.Family.CLIENT_ERROR
    override fun getReasonPhrase(): String = "Unprocessable Content"
}
