//package com.cassisi.openeventstore.core
//
//import com.apple.foundationdb.*
//import com.apple.foundationdb.directory.DirectoryLayer
//import com.cassisi.openeventstore.core.prototype.chatgpt.PropertyBag
//import kotlinx.serialization.Serializable
//import kotlinx.serialization.json.*
//
//enum class FieldType {
//    STRING, INT, LONG, DOUBLE, BOOLEAN, TIMESTAMP
//}
//
//data class FieldId(val id: Int)
//
//sealed class PropertyValue {
//    data class StringVal(val value: String) : PropertyValue()
//    data class IntVal(val value: Int) : PropertyValue()
//    data class LongVal(val value: Long) : PropertyValue()
//    data class DoubleVal(val value: Double) : PropertyValue()
//    data class BoolVal(val value: Boolean) : PropertyValue()
//    data class TimestampVal(val epochMillis: Long) : PropertyValue()
//
//    companion object {
//
//        // --- PropertyValue helper ---
//        fun fromAny(value: Any): PropertyValue = when (value) {
//            is String -> PropertyValue.StringVal(value)
//            is Int -> PropertyValue.IntVal(value)
//            is Long -> PropertyValue.LongVal(value)
//            is Double -> PropertyValue.DoubleVal(value)
//            is Boolean -> PropertyValue.BoolVal(value)
//            else -> throw IllegalArgumentException("Unsupported type $value")
//        }
//
//    }
//}
//
//class PropertyBag {
//    private val values: MutableMap<FieldId, MutableList<PropertyValue>> = mutableMapOf()
//
//    fun add(fieldId: FieldId, value: PropertyValue) {
//        values.computeIfAbsent(fieldId) { mutableListOf() }.add(value)
//    }
//
//    fun get(fieldId: FieldId): List<PropertyValue> =
//        values[fieldId] ?: emptyList()
//
//    fun all(): Map<FieldId, List<PropertyValue>> =
//        values.toMap()
//}
//
//data class FieldDef(val id: FieldId, val type: FieldType, val path: String)
//
//class SchemaRegistry {
//    private val pathToField = mutableMapOf<String, FieldDef>()
//
//    fun register(path: String, id: Int, type: FieldType) {
//        pathToField[path] = FieldDef(FieldId(id), type, path)
//    }
//
//  // fun resolve(path: String): FieldDef? = pathToField[path]
//  fun resolve(path: String): FieldDef? {
//      // Exact match
//      pathToField[path]?.let { return it }
//
//      // Try wildcard match: replace [<index>] with []
//      val wildcardPath = path.replace(Regex("\\[\\d+\\]"), "[]")
//      return pathToField[wildcardPath]
//  }
//
//    fun dump() {
//        pathToField.forEach{println(it)}
//    }
//}
//
//
//object JsonNormalizer {
//    private val json = Json { ignoreUnknownKeys = true }
//
//    fun normalize(jsonStr: String, schema: SchemaRegistry): PropertyBag {
//        val root = json.parseToJsonElement(jsonStr)
//        val bag = PropertyBag()
//
//        fun traverse(node: JsonElement, path: String) {
//            when (node) {
//                is JsonObject -> {
//                    node.forEach { (k, v) ->
//                        traverse(v, if (path.isEmpty()) k else "$path.$k")
//                    }
//                }
//                is JsonArray -> {
//                    node.forEachIndexed { idx, elem ->
//                        // Encode the index in the path to distinguish array elements
//                        traverse(elem, "$path[$idx]")
//                    }
//                }
//                is JsonPrimitive -> {
//                    schema.resolve(path)?.let { fieldDef ->
//                        val value = when (fieldDef.type) {
//                            FieldType.STRING -> PropertyValue.StringVal(node.content)
//                            FieldType.INT -> PropertyValue.IntVal(node.int)
//                            FieldType.LONG -> PropertyValue.LongVal(node.long)
//                            FieldType.DOUBLE -> PropertyValue.DoubleVal(node.double)
//                            FieldType.BOOLEAN -> PropertyValue.BoolVal(node.boolean)
//                            FieldType.TIMESTAMP -> PropertyValue.TimestampVal(node.long)
//                        }
//                        bag.add(fieldDef.id, value)
//                    }
//                }
//            }
//        }
//
//        traverse(root, "")
//        return bag
//    }
//}
//
//object SchemaInferer {
//    fun infer(jsonStr: String, startingId: Int = 100): SchemaRegistry {
//        val root = Json.parseToJsonElement(jsonStr)
//        val schema = SchemaRegistry()
//        var counter = startingId
//
//        fun traverse(node: JsonElement, path: String) {
//            when (node) {
//                is JsonObject -> {
//                    node.forEach { (k, v) ->
//                        val newPath = if (path.isEmpty()) k else "$path.$k"
//                        traverse(v, newPath)
//                    }
//                }
//                is JsonArray -> {
//                    node.forEachIndexed { idx, elem ->
//                        val newPath = "$path[]." + when (elem) {
//                            is JsonObject, is JsonArray -> "" // handled by deeper recursion
//                            else -> "" // just fall through
//                        }
//                        traverse(elem, newPath.removeSuffix("."))
//                    }
//                }
//                is JsonPrimitive -> {
//                    val type = when {
//                        node.isString -> FieldType.STRING
//                        node.booleanOrNull != null -> FieldType.BOOLEAN
//                        node.longOrNull != null -> FieldType.LONG
//                        node.intOrNull != null -> FieldType.INT
//                        node.doubleOrNull != null -> FieldType.DOUBLE
//                        else -> FieldType.STRING
//                    }
//                    if (schema.resolve(path) == null) {
//                        schema.register(path, counter++, type)
//                    }
//                }
//            }
//        }
//
//        traverse(root, "")
//        return schema
//    }
//}
//
////fun main() {
////    val jsonPayload = """
////        {
////          "username": "john_doe",
////          "email": "john.doe@example.com",
////          "user": { "address": { "city": "Paris" } },
////          "age": 27,
////          "roles": [
////            { "name": "admin", "level": 5 },
////            { "name": "editor", "level": 3 }
////          ]
////        }
////    """
////
////    val schema = SchemaInferer.infer(jsonPayload)
////
////    val bag = JsonNormalizer.normalize(jsonPayload, schema)
////
////    schema.dump() // just a helper to print paths + IDs
////    println("---- PropertyBag ----")
////    bag.all().forEach { (fieldId, values) ->
////        println("Field ${fieldId.id}: $values")
////    }
////}
//
//// --- Operators ---
//enum class Operator { EQ, NEQ, LT, LTE, GT, GTE, IN }
//
//// --- Filter on a single field ---
//data class FieldFilter(
//    val fieldId: FieldId,
//    val op: Operator,
//    val value: PropertyValue
//)
//
//// --- Event-type scoped filter ---
//data class EventTypeFilter(
//    val eventType: String,
//    val filters: List<FieldFilter> = emptyList()
//)
//
//// --- Top-level query supporting OR of multiple event-type filters ---
//data class EventQuery(
//    val clauses: List<EventTypeFilter> = emptyList()
//)
//
//// --- DSL Builders ---
//class EventQueryBuilder {
//    private val clauses = mutableListOf<EventTypeFilter>()
//
//    fun eventType(name: String, block: EventTypeFilterBuilder.() -> Unit) {
//        val builder = EventTypeFilterBuilder(name)
//        builder.block()
//        clauses.add(builder.build())
//    }
//
//    fun build() = EventQuery(clauses)
//}
//
//class EventTypeFilterBuilder(private val name: String) {
//    private val filters = mutableListOf<FieldFilter>()
//
//    fun eq(fieldId: FieldId, value: Any) {
//        filters.add(FieldFilter(fieldId, Operator.EQ, PropertyValue.fromAny(value)))
//    }
//
//    fun gt(fieldId: FieldId, value: Any) {
//        filters.add(FieldFilter(fieldId, Operator.GT, PropertyValue.fromAny(value)))
//    }
//
//    // Add more operators as needed...
//
//    fun build() = EventTypeFilter(name, filters)
//}
//
//// --- DSL Entry point ---
//fun query(block: EventQueryBuilder.() -> Unit): EventQuery =
//    EventQueryBuilder().apply(block).build()
//
//val q = query {
//    eventType("USER_ONBOARDED") { eq(FieldId(101), "test123") }
//    eventType("TODO_LIST_ADDED") { eq(FieldId(201), 1234) }
//}
//
//@Serializable
//data class EventId(val id: Long)
//@Serializable
//data class EventType(val name: String)
//@Serializable
//data class Event(
//    val id: EventId,
//    val type: EventType,
//    val timestamp: Long,
//    @kotlinx.serialization.Transient
//    val payload: PropertyBag = PropertyBag()
//)
//
//class FdbEventStore(private val db: Database) {
//
//    private val directory = DirectoryLayer.getDefault().createOrOpen(db, listOf("events")).get()
//
//    // --- Save event ---
//    fun saveEvent(event: Event) {
//        db.run { tr ->
//            val key = directory.pack(listOf(event.type.name, event.id.id))
//            tr[key] = serializeEvent(event)
//
//            // Index each property in payload
//            event.payload.all().forEach { (fieldId, values) ->
//                values.forEach { value ->
//                    val idxKey = directory.pack(listOf("index", event.type.name, fieldId.id, value.toString(), event.id.id))
//                    tr[idxKey] = ByteArray(0)
//                }
//            }
//        }
//    }
//
//    // --- Query using DSL ---
//    fun query(q: EventQuery): List<EventId> {
//        return db.read { tr ->
//            val clauseResults: List<Set<EventId>> = q.clauses.map { clause ->
//                queryEventTypeClause(tr, clause)
//            }
//            // OR semantics across clauses
//            clauseResults.flatten().toSet().toList()
//        }
//    }
//
//    // --- Query a single EventTypeFilter (AND semantics) ---
//    private fun queryEventTypeClause(tr: ReadTransaction, clause: EventTypeFilter): Set<EventId> {
//        if (clause.filters.isEmpty()) return fetchAllEventsOfType(tr, clause.eventType)
//
//        val sets: List<Set<EventId>> = clause.filters.map { f ->
//            val prefix = directory.pack(listOf("index", clause.eventType, f.fieldId.id, f.value.toString()))
//            val endKey = fdbRangeEnd(prefix)
//
//            // Use standard transaction read
//            val snapshot = tr.getRange(prefix, endKey, 1000).asList().get()
//            snapshot.map { EventId(directory.unpack(it.key)[4] as Long) }.toSet()
//        }
//
//        return sets.reduce { acc, s -> acc.intersect(s) }
//    }
//
//    private fun fetchAllEventsOfType(tr: ReadTransaction, eventType: String): Set<EventId> {
//        val prefix = directory.pack(listOf(eventType))
//        val endKey = fdbRangeEnd(prefix)
//
//        val snapshot = tr.getRange(prefix, endKey, 1000).asList().get()
//        return snapshot.map { EventId(directory.unpack(it.key)[1] as Long) }.toSet()
//    }
//
//    private fun fdbRangeEnd(prefix: ByteArray): ByteArray {
//        // Find the first byte from the end that is < 0xFF and increment it
//        val end = prefix.copyOf()
//        for (i in end.size - 1 downTo 0) {
//            if (end[i].toInt() and 0xFF != 0xFF) {
//                end[i] = (end[i] + 1).toByte()
//                return end.copyOfRange(0, i + 1)
//            }
//        }
//        // If all bytes are 0xFF, append 0x00
//        return prefix + 0x00
//    }
//
//    private fun serializeEvent(event: Event): ByteArray {
//        return Json.encodeToString(event).toByteArray()
//        //return JsonNormalizer.toJsonBytes(event.payload)
//    }
//}
//
//fun main() {
//    FDB.selectAPIVersion(730)
//    val fdb = FDB.instance()
//    val db = fdb.open("/etc/foundationdb/fdb.cluster")
//
//
//    val usernameField = FieldId(101)
//    val todoIdField = FieldId(201)
//
//    val userOnboardedType = EventType("USER_ONBOARDED")
//    val todoListAddedType = EventType("TODO_LIST_ADDED")
//
//    val userEvent = Event(
//        id = EventId(1),
//        type = userOnboardedType,
//        timestamp = System.currentTimeMillis(),
//        payload = PropertyBag().apply {
//            add(usernameField, PropertyValue.StringVal("test123"))
//            add(FieldId(102), PropertyValue.StringVal("user@example.com"))
//        }
//    )
//
//    val todoEvent = Event(
//        id = EventId(2),
//        type = todoListAddedType,
//        timestamp = System.currentTimeMillis(),
//        payload = PropertyBag().apply {
//            add(todoIdField, PropertyValue.IntVal(1234))
//            add(FieldId(202), PropertyValue.StringVal("Buy milk"))
//        }
//    )
//
//    val eventStore = FdbEventStore(db)
//
//    eventStore.saveEvent(userEvent)
//    eventStore.saveEvent(todoEvent)
//
//    val q = query {
//        eventType("USER_ONBOARDED") {
//      //      eq(usernameField, "test123")
//        }
//        eventType("TODO_LIST_ADDED") {
//            eq(todoIdField, 1234)
//        }
//    }
//
//// Execute query
//    val matchingEvents: List<EventId> = eventStore.query(q)
//
//    println("Found events: $matchingEvents")
//}