package com.cassisi.openeventstore.core.prototype

import com.apple.foundationdb.Database
import com.cassisi.openeventstore.core.prototype.chatgpt.FieldId
import com.cassisi.openeventstore.core.prototype.chatgpt.PropertyBag
import java.util.UUID

class PrototypeEventStore(
    private val db: Database
) {


   // fun save(events: List<FdbEvent>)

}

data class FieldId(val id: String)

data class PropertyBag(
    val propertyMap: Map<FieldId, Any>
)

data class FdbEvent(
    val eventId: UUID,
    val eventType: String,
    val payload: String,
    val propertyBag: PropertyBag
)