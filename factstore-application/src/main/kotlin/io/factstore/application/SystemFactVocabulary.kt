package io.factstore.application

import io.factstore.core.FactType
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import java.util.UUID

object SystemFactTypes {
    val STORE_CREATED = FactType("factstore.StoreCreated")
    val STORE_CREATION_REJECTED = FactType("factstore.StoreCreationRejected")
}

object SystemFactTags {
    val STORE_ID = TagKey("storeId")
    val STORE_NAME = TagKey("storeName")

    fun storeId(storeId: StoreId): Pair<TagKey, TagValue> = STORE_ID to TagValue(storeId.text)

    fun storeName(storeName: StoreName): Pair<TagKey, TagValue> = STORE_NAME to TagValue(storeName.value)
}

internal val StoreId.text: String get() = uuid.toString()

internal fun String.toStoreId(): StoreId = StoreId(UUID.fromString(this))
