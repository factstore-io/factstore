package io.factstore.avro

import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.toTagKey
import io.factstore.core.toTagValue
import kotlinx.serialization.*
import io.factstore.core.*
import java.time.Instant
import kotlin.annotation.AnnotationRetention.RUNTIME
import kotlin.annotation.AnnotationTarget.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.*

class AvroFdbStore(
    private val factStore: io.factstore.core.FactStore
) {

    suspend fun <T : Any> append(fact: T) =
        factStore.append(fact = _root_ide_package_.io.factstore.avro.FactRegistry.toEnvelope(fact))

    suspend fun <T : Any> append(facts: List<T>, condition: io.factstore.core.AppendCondition.TagQueryBased) =
        facts
            .map { _root_ide_package_.io.factstore.avro.FactRegistry.toEnvelope(it) }
            .also {
                factStore.append(
                    _root_ide_package_.io.factstore.core.AppendRequest(
                        facts = it,
                        idempotencyKey = _root_ide_package_.io.factstore.core.IdempotencyKey(),
                        condition = condition
                    )
                )
            }


    suspend fun readSubject(type: String, id: String): List<Any> =
        factStore
            .findBySubject(_root_ide_package_.io.factstore.core.SubjectRef(type, id))
            .map { _root_ide_package_.io.factstore.avro.FactRegistry.fromEnvelope(it) }

    suspend fun readFromTagQuery(tagQuery: io.factstore.core.TagQuery): List<Pair<io.factstore.core.FactId, Any>> =
        factStore
            .findByTagQuery(tagQuery)
            .map { Pair(it.id, _root_ide_package_.io.factstore.avro.FactRegistry.fromEnvelope(it)) }
}

@Retention(RUNTIME)
@Target(CLASS)
annotation class FactType(val name: String)

@Retention(RUNTIME)
@Target(PROPERTY)
annotation class Tag(val name: String)

@Retention(RUNTIME)
@Target(CLASS)
annotation class SubjectType(val value: String)

@Retention(RUNTIME)
@Target(PROPERTY)
annotation class SubjectId

data class FactDescriptor<T : Any>(
    val type: String,
    val version: Int = 1,
    val clazz: KClass<T>,
    val serializer: io.factstore.avro.FactSerde<T>
)

inline fun <reified T : Any> createAvroFactDescriptor(type: String, version: Int = 1): io.factstore.avro.FactDescriptor<T> =
    _root_ide_package_.io.factstore.avro.FactDescriptor(
        type = type,
        version = version,
        clazz = T::class,
        serializer = _root_ide_package_.io.factstore.avro.FactAvroSerde.create<T>()
    )

object FactRegistry {

    private val byType = mutableMapOf<String, io.factstore.avro.FactDescriptor<*>>()
    private val byClass = mutableMapOf<KClass<*>, io.factstore.avro.FactDescriptor<*>>()

    fun <T : Any> register(descriptor: io.factstore.avro.FactDescriptor<T>) {
        byType[descriptor.type] = descriptor
        byClass[descriptor.clazz] = descriptor

        // TODO: validate (check for missing annotations etc.)
    }

    fun fromEnvelope(fact: io.factstore.core.Fact): Any {
        val descriptor = byType[fact.type.value] ?: error("Unknown fact type ${fact.type}")
        val serde = (descriptor as io.factstore.avro.FactDescriptor<Any>).serializer
        return serde.deserialize(fact.payload.data)
    }

    fun toEnvelope(fact: Any): io.factstore.core.Fact {
        return toFact(fact)
    }

    private fun toFact(fact: Any): io.factstore.core.Fact {
        val factDescriptor = byClass[fact::class] ?: error("No FactDescriptor found for class ${fact::class}")
        val factSerde = (factDescriptor as io.factstore.avro.FactDescriptor<Any>).serializer

        val classType = fact::class
        val factType = classType.findAnnotation<io.factstore.avro.FactType>()?.name ?: classType.simpleName
        ?: throw IllegalArgumentException("Cannot extract simple name")
        val subjectType =
            classType.findAnnotation<io.factstore.avro.SubjectType>()?.value ?: throw IllegalArgumentException("Missing SubjectType")
        println(subjectType)

        val subjectId = classType
            .memberProperties
            .first { it.hasAnnotation<io.factstore.avro.SubjectId>() }
            .let { (it as KProperty1<Any, *>).get(fact) }
            .toString()

        val tags = classType.memberProperties
            .mapNotNull {
                val key = it.findAnnotation<io.factstore.avro.Tag>()?.name
                if (key != null) {
                    val value = (it as KProperty1<Any, *>).get(fact).toString()
                    Pair(key, value)
                } else {
                    null
                }
            }
            .toMap()

        val fact = _root_ide_package_.io.factstore.core.Fact(
            id = _root_ide_package_.io.factstore.core.FactId.generate(),
            type = _root_ide_package_.io.factstore.core.FactType(factType),
            payload = _root_ide_package_.io.factstore.core.FactPayload(factSerde.serialize(fact)),
            subjectRef = _root_ide_package_.io.factstore.core.SubjectRef(
                type = subjectType,
                id = subjectId
            ),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = tags.entries.associate { it.key.toTagKey() to it.value.toTagValue() }
        )
        return fact
    }

}

interface FactSerde<T> {

    fun serialize(fact: T): ByteArray

    fun deserialize(byteArray: ByteArray): T

}

class FactAvroSerde<T>(val serializer: KSerializer<T>) : io.factstore.avro.FactSerde<T> {

    override fun serialize(fact: T): ByteArray =
        Avro.encodeToByteArray(serializer, fact)

    override fun deserialize(byteArray: ByteArray): T =
        Avro.decodeFromByteArray(serializer, byteArray)

    companion object {
        inline fun <reified T> create(): FactAvroSerde<T> = FactAvroSerde(serializer())
    }
}
