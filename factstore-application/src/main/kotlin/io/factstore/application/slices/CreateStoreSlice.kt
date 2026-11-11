package io.factstore.application.slices

import com.fraktalio.fmodel.domain.IDecider
import io.factstore.application.FactRepository
import io.factstore.application.RequestEncoder
import io.factstore.application.RequestEncoding
import io.factstore.application.RequestHandler
import io.factstore.application.RetryPolicy
import io.factstore.application.Slice
import io.factstore.application.StoreIdGenerator
import io.factstore.application.SystemConsistencyBoundary
import io.factstore.application.SystemFactTags
import io.factstore.application.SystemFactTypes
import io.factstore.application.SystemStore
import io.factstore.core.FactId
import io.factstore.core.FactQueryItem
import io.factstore.core.StoreName
import io.factstore.domain.CreateStore
import io.factstore.domain.CreateStoreDecisionModel
import io.factstore.domain.SystemFact
import io.factstore.storage.LogCondition
import io.factstore.storage.LogQuery
import io.factstore.storage.LogQueryItem

data class CreateStoreRequest(val storeName: StoreName)

fun CreateStoreRequest.toCommand(storeIds: StoreIdGenerator): CreateStore = CreateStore(storeIds.generate(), storeName)

object CreateStoreRequestEncoding : RequestEncoding<CreateStoreRequest> {

    override val typeName: String = "CreateStore"

    override fun encode(request: CreateStoreRequest, encoder: RequestEncoder) {
        encoder.string(request.storeName.value)
    }
}

typealias CreateStoreSlice = Slice<CreateStore, CreateStoreDecisionModel, SystemFact, LogQuery, LogCondition, FactId>

fun CreateStoreSlice(
    decider: IDecider<CreateStore, CreateStoreDecisionModel, SystemFact>,
    repository: FactRepository<SystemFact, LogQuery, LogCondition, FactId>,
): CreateStoreSlice = Slice(decider, CreateStoreConsistencyBoundary, repository)

typealias CreateStoreRequestHandler = RequestHandler<CreateStoreRequest, SystemFact, FactId>

fun CreateStoreRequestHandler(
    slice: CreateStoreSlice,
    storeIds: StoreIdGenerator,
    retryPolicy: RetryPolicy,
): CreateStoreRequestHandler = RequestHandler(slice, CreateStoreRequestEncoding, { it.toCommand(storeIds) }, retryPolicy)

private object CreateStoreConsistencyBoundary : SystemConsistencyBoundary<CreateStore> {

    override fun query(command: CreateStore): LogQuery = LogQuery(
        LogQueryItem(
            storeId = SystemStore.ID,
            item = FactQueryItem(
                types = setOf(SystemFactTypes.STORE_CREATED),
                tags = mapOf(SystemFactTags.storeName(command.storeName)),
            ),
        )
    )
}
