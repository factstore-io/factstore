package org.factstore.avro
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.*
import org.factstore.core.*
import java.time.Instant
import kotlin.annotation.AnnotationRetention.RUNTIME
import kotlin.annotation.AnnotationTarget.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.*

class AvroFdbStore(
    private val factStore: FactStore
) {

    suspend fun <T : Any> append(fact: T) =
        factStore.append(fact = FactRegistry.toEnvelope(fact))

    suspend fun <T : Any> append(facts: List<T>, condition: TagQueryBasedAppendCondition) =
        facts
            .map { FactRegistry.toEnvelope(it) }
            .also { factStore.append(it, condition) }


    suspend fun readSubject(type: String, id: String): List<Any> =
        factStore
            .findBySubject(type, id)
            .map { FactRegistry.fromEnvelope(it) }

    suspend fun readFromTagQuery(tagQuery: TagQuery): List<Pair<FactId, Any>> =
        factStore
            .findByTagQuery(tagQuery)
            .map { Pair(it.id, FactRegistry.fromEnvelope(it)) }
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
    val serializer: FactSerde<T>
)

inline fun <reified T : Any> createAvroFactDescriptor(type: String, version: Int = 1): FactDescriptor<T> =
    FactDescriptor(
        type = type,
        version = version,
        clazz = T::class,
        serializer = FactAvroSerde.create<T>()
    )

object FactRegistry {

    private val byType = mutableMapOf<String, FactDescriptor<*>>()
    private val byClass = mutableMapOf<KClass<*>, FactDescriptor<*>>()

    fun <T : Any> register(descriptor: FactDescriptor<T>) {
        byType[descriptor.type] = descriptor
        byClass[descriptor.clazz] = descriptor

        // TODO: validate (check for missing annotations etc.)
    }

    fun fromEnvelope(fact: Fact): Any {
        val descriptor = byType[fact.type] ?: error("Unknown fact type ${fact.type}")
        val serde = (descriptor as FactDescriptor<Any>).serializer
        return serde.deserialize(fact.payload)
    }

    fun toEnvelope(fact: Any): Fact {
        return toFact(fact)
    }

    private fun toFact(fact: Any): Fact {
        val factDescriptor = byClass[fact::class] ?: error("No FactDescriptor found for class ${fact::class}")
        val factSerde = (factDescriptor as FactDescriptor<Any>).serializer

        val classType = fact::class
        val factType = classType.findAnnotation<FactType>()?.name ?: classType.simpleName
        ?: throw IllegalArgumentException("Cannot extract simple name")
        val subjectType =
            classType.findAnnotation<SubjectType>()?.value ?: throw IllegalArgumentException("Missing SubjectType")
        println(subjectType)

        val subjectId = classType
            .memberProperties
            .first { it.hasAnnotation<SubjectId>() }
            .let { (it as KProperty1<Any, *>).get(fact) }
            .toString()

        val tags = classType.memberProperties
            .mapNotNull {
                val key = it.findAnnotation<Tag>()?.name
                if (key != null) {
                    val value = (it as KProperty1<Any, *>).get(fact).toString()
                    Pair(key, value)
                } else {
                    null
                }
            }
            .toMap()

        val fact = Fact(
            id = FactId.generate(),
            type = factType,
            payload = factSerde.serialize(fact),
            subject = Subject(
                type = subjectType,
                id = subjectId
            ),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = tags
        )
        return fact
    }

}

interface FactSerde<T> {

    fun serialize(fact: T): ByteArray

    fun deserialize(byteArray: ByteArray): T

}

class FactAvroSerde<T>(val serializer: KSerializer<T>) : FactSerde<T> {

    override fun serialize(fact: T): ByteArray =
        Avro.encodeToByteArray(serializer, fact)

    override fun deserialize(byteArray: ByteArray): T =
        Avro.decodeFromByteArray(serializer, byteArray)

    companion object {
        inline fun <reified T> create(): FactAvroSerde<T> = FactAvroSerde(serializer())
    }
}
