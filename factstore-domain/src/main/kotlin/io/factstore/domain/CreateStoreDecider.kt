package io.factstore.domain

import com.fraktalio.fmodel.domain.Decider
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import kotlinx.coroutines.flow.flowOf

data class CreateStoreDecisionModel(val holders: Map<StoreName, StoreId> = emptyMap())

typealias CreateStoreDecider = Decider<CreateStore, CreateStoreDecisionModel, SystemFact>

val createStoreDecider: CreateStoreDecider = Decider(
    decide = { command, model ->
        when (val holder = model.holders[command.storeName]) {
            null -> flowOf(StoreCreated(command.storeId, command.storeName))
            else -> flowOf(
                StoreCreationRejected(command.storeId, command.storeName, StoreCreationRejected.Reason.NameTaken(holder))
            )
        }
    },
    evolve = { model, fact ->
        when (fact) {
            is StoreCreated -> model.copy(holders = model.holders + (fact.storeName to fact.storeId))
            else -> model
        }
    },
    initialState = CreateStoreDecisionModel(),
)
