package io.factstore.cli.client

import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.rest.client.ext.ResponseExceptionMapper

class FactStoreClientExceptionMapper : ResponseExceptionMapper<FactStoreApiException> {

    override fun toThrowable(response: Response): FactStoreApiException {
        return runCatching {
            response.readEntity(ApiError::class.java)
        }.fold(
            onSuccess = { apiError ->
                FactStoreApiException(apiError, response.status)
            },
            onFailure = { _ ->
                FactStoreApiException(null, response.status)
            }
        )
    }

}

data class FactStoreApiException(
    val apiError: ApiError?,
    val httpStatus: Int,
) : RuntimeException()

data class ApiError(
    val status: String = "Failure",
    val message: String,
    val reason: String,
    val code: Int,
    val details: Map<String, Any?>? = null,
)
