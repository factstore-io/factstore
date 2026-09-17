package io.factstore.server.http

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import io.factstore.server.input.InvalidInputException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.ws.rs.WebApplicationException
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.BAD_REQUEST
import jakarta.ws.rs.core.Response.Status.Family.SERVER_ERROR
import jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR
import jakarta.ws.rs.core.Response.Status.NOT_FOUND
import org.jboss.resteasy.reactive.server.ServerExceptionMapper
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Translates failures into [ApiError] responses, so that every error of the HTTP API has a JSON body.
 *
 * Domain outcomes, such as a missing store, are not failures; the resources render those themselves.
 */
class ErrorMappers {

    @ServerExceptionMapper
    fun invalidInput(e: InvalidInputException): Response =
        apiErrorResponse(BAD_REQUEST, Reason.InvalidInput, e.message)

    /** A body that is not JSON, or does not have the shape the endpoint expects. */
    @ServerExceptionMapper(JsonProcessingException::class, MismatchedInputException::class)
    fun malformedBody(e: JsonProcessingException): Response {
        val message = (e as? JsonMappingException)?.fieldPath
            ?.let { "The request body has a missing or invalid value at '$it'." }
            ?: "The request body is not valid JSON."
        return apiErrorResponse(BAD_REQUEST, Reason.InvalidInput, message)
    }

    @ServerExceptionMapper
    fun streamOutcome(e: StreamApiException): Response = when (e) {
        is StreamApiException.StoreNotFoundException -> storeNotFoundError(e.storeName)
        is StreamApiException.FactNotFoundException -> factNotFoundError(e.factId)
    }

    /** Responses produced by the framework, such as 404, 405 or 415, keep their status and headers. */
    @ServerExceptionMapper
    fun framework(e: WebApplicationException): Response {
        val status = e.response.statusInfo
        val reason = when {
            status.statusCode == NOT_FOUND.statusCode -> Reason.NotFound
            status.family == SERVER_ERROR -> Reason.InternalError
            else -> Reason.InvalidInput
        }
        val error = ApiError(message = status.reasonPhrase, reason = reason, code = status.statusCode)
        return Response.fromResponse(e.response).entity(error).type(APPLICATION_JSON).build()
    }

    @ServerExceptionMapper
    fun unexpected(e: Exception): Response {
        val errorId = UUID.randomUUID()
        logger.error(e) { "Unexpected error $errorId" }
        return apiErrorResponse(
            status = INTERNAL_SERVER_ERROR,
            reason = Reason.InternalError,
            message = "An unexpected error occurred.",
            details = mapOf("errorId" to errorId),
        )
    }
}

/** Where in the body the offending value is, such as `facts[0].payload`, without internal class names. */
private val JsonMappingException.fieldPath: String?
    get() = path
        .joinToString("") { reference -> reference.fieldName?.let { ".$it" } ?: "[${reference.index}]" }
        .removePrefix(".")
        .ifEmpty { null }
