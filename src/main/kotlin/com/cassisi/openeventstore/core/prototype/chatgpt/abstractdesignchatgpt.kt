package com.cassisi.openeventstore.core.prototype.chatgpt

import com.cassisi.openeventstore.core.prototype.FieldId
import com.cassisi.openeventstore.core.prototype.PropertyBag

interface PropertyIndexer<T : PropertyBag> {
    // Update index when saving an event
    fun index(event: Event<T>)

    // Query index for fast lookups
    fun lookup(eventType: String, fieldId: FieldId, value: PropertyValue): Set<EventId>
}

data class Filter(val fieldId: FieldId, val value: PropertyValue)
data class EventTypeFilter(val eventType: String, val filters: List<Filter> = emptyList())

class EventQuery(val clauses: List<EventTypeFilter>)

fun query(build: QueryBuilder.() -> Unit): EventQuery {
    val builder = QueryBuilder().apply(build)
    return EventQuery(builder.clauses)
}

class QueryBuilder {
    val clauses = mutableListOf<EventTypeFilter>()
    fun eventType(name: String, block: FilterBuilder.() -> Unit = {}) {
        val fb = FilterBuilder().apply(block)
        clauses.add(EventTypeFilter(name, fb.filters))
    }
}

class FilterBuilder {
    val filters = mutableListOf<Filter>()
    fun eq(fieldId: FieldId, value: Any) {
        val propValue = when (value) {
            is String -> PropertyValue.StringVal(value)
            is Int -> PropertyValue.IntVal(value)
            is Long -> PropertyValue.LongVal(value)
            is Boolean -> PropertyValue.BooleanVal(value)
            is Double -> PropertyValue.DoubleVal(value)
            else -> error("Unsupported type")
        }
        filters.add(Filter(fieldId, propValue))
    }
}

interface EventStore<T : PropertyBag> {
    fun saveEvent(event: Event<T>)
    fun query(query: EventQuery): List<EventId>
}


interface EventSerializer<T : PropertyBag> {
    fun serialize(event: Event<T>): ByteArray
    fun deserialize(data: ByteArray): Event<T>
}

// Example generic implementation can wrap JSON/Avro/Protobuf without changing the store

// A unique identifier for an event
data class EventId(val id: Long)

// Type-safe event type
data class EventType(val name: String)

// Generic property value (can be extended for more types)
sealed interface PropertyValue {
    data class StringVal(val value: String) : PropertyValue
    data class IntVal(val value: Int) : PropertyValue
    data class LongVal(val value: Long) : PropertyValue
    data class BooleanVal(val value: Boolean) : PropertyValue
    data class DoubleVal(val value: Double) : PropertyValue
}

// Property identifier (abstracted from serialization)
data class FieldId(val id: Int)

// Container for event properties
class PropertyBag {
    private val properties = mutableMapOf<FieldId, MutableList<PropertyValue>>()

    fun add(fieldId: FieldId, value: PropertyValue) {
        properties.computeIfAbsent(fieldId) { mutableListOf() }.add(value)
    }

    fun all(): Map<FieldId, List<PropertyValue>> = properties
}

// Core event class (generic payload type T)
data class Event<T : PropertyBag>(
    val id: EventId,
    val type: EventType,
    val timestamp: Long,
    val payload: T
)



