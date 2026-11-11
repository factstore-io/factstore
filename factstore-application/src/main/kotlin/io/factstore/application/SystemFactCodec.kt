package io.factstore.application

import io.factstore.core.Fact
import io.factstore.core.FactInput
import io.factstore.core.FactPayload
import io.factstore.core.FactType
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import io.factstore.domain.StoreCreated
import io.factstore.domain.StoreCreationRejected
import io.factstore.domain.SystemFact
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

object SystemFactCodec {

    fun encode(fact: SystemFact): FactInput = when (fact) {
        is StoreCreated -> factInput(
            SystemFactTypes.STORE_CREATED, fact.storeId,
            StoreCreatedPayload(fact.storeId.text, fact.storeName.value),
            SystemFactTags.storeName(fact.storeName),
        )
        is StoreCreationRejected -> factInput(
            SystemFactTypes.STORE_CREATION_REJECTED, fact.storeId,
            fact.reason.toPayload(fact.storeId, fact.storeName),
            SystemFactTags.storeName(fact.storeName),
        )
    }

    fun decode(fact: Fact): SystemFact = when (fact.type) {
        SystemFactTypes.STORE_CREATED -> fact.payload<StoreCreatedPayload>().run {
            StoreCreated(storeId.toStoreId(), StoreName(storeName))
        }
        SystemFactTypes.STORE_CREATION_REJECTED -> fact.payload<StoreCreationRejectedPayload>().run {
            StoreCreationRejected(storeId.toStoreId(), StoreName(storeName), creationReason())
        }
        else -> throw IllegalArgumentException("A fact of type '${fact.type.value}' is not one of FactStore's own facts.")
    }

    private inline fun <reified P> factInput(type: FactType, storeId: StoreId, payload: P, vararg tags: Pair<TagKey, TagValue>) =
        FactInput(
            type = type,
            subject = Subject(storeId.text),
            payload = FactPayload(Json.encodeToString(payload).encodeToByteArray()),
            tags = mapOf(SystemFactTags.storeId(storeId), *tags),
        )

    private inline fun <reified P> Fact.payload(): P = Json.decodeFromString(payload.data.decodeToString())
}

private const val NAME_TAKEN = "NAME_TAKEN"

private fun StoreCreationRejected.Reason.toPayload(storeId: StoreId, storeName: StoreName) = when (this) {
    is StoreCreationRejected.Reason.NameTaken ->
        StoreCreationRejectedPayload(storeId.text, storeName.value, NAME_TAKEN, holder.text)
}

private fun StoreCreationRejectedPayload.creationReason(): StoreCreationRejected.Reason = when (reason) {
    NAME_TAKEN -> StoreCreationRejected.Reason.NameTaken(holder.toStoreId())
    else -> throw IllegalArgumentException("Unknown rejection reason '$reason'.")
}

@Serializable
private data class StoreCreatedPayload(val storeId: String, val storeName: String)

@Serializable
private data class StoreCreationRejectedPayload(
    val storeId: String,
    val storeName: String,
    val reason: String,
    val holder: String,
)
