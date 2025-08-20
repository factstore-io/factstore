package com.cassisi.openeventstore

import com.apple.foundationdb.Database
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.launch
import kotlinx.serialization.json.*
import java.util.concurrent.CompletableFuture
import javax.management.Query

const val ROOT_DIRECTORY_NAME = "event-store"
const val NAMESPACE_EVENTS = "events"
const val NAMESPACE_TYPE_INDEX = "type-index"
const val NAMESPACE_TAG_INDEX = "tag-index"
const val NAMESPACE_COMPOSITE_INDEX = "composite-index"

const val WILDCARD = "[]"

enum class TypeTag(val code: Byte) {
    STRING('s'.code.toByte()),
    NUMBER('n'.code.toByte()),
    BOOL('b'.code.toByte()),
    NULL('z'.code.toByte()),
    FOLDED_STRING('S'.code.toByte())
}

data class Event(
    val type: String,
    val metadata: Map<String, String> = emptyMap(),
    val payload: JsonElement,
)

data class IndexConfig(
    val rangePaths: Set<List<String>> = emptySet(),
    val includePaths: Set<List<String>>? = null,       // null = all
    val excludePaths: Set<List<String>> = emptySet(),
    val maxTermsPerEvent: Int = 5000,
    val maxDepth: Int = 32,
    val foldCasePaths: Set<List<String>> = emptySet(), // optional folded variant
)

sealed interface Term

data class GinTerm(
    val typeTag: TypeTag,
    val path: List<String>,
    val valueKey: ByteArray,
) : Term {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as GinTerm

        if (typeTag != other.typeTag) return false
        if (path != other.path) return false
        if (!valueKey.contentEquals(other.valueKey)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = typeTag.hashCode()
        result = 31 * result + path.hashCode()
        result = 31 * result + valueKey.contentHashCode()
        return result
    }
}

data class GpxTerm(
    val path: List<String>,
) : Term

data class GrgTerm(
    val path: List<String>,
    val rangeCode: ByteArray,
) : Term {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as GrgTerm

        if (path != other.path) return false
        if (!rangeCode.contentEquals(other.rangeCode)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = path.hashCode()
        result = 31 * result + rangeCode.contentHashCode()
        return result
    }
}

// --- Normalization helpers ---
object Normalizers {
    fun eqString(s: String): ByteArray = s.encodeToByteArray()

    fun eqNumberCanonical(n: String): ByteArray {
        // Very simple canonicalizer; replace with a robust BigDecimal-based impl in prod
        val bd = java.math.BigDecimal(n)
        val canon = bd.stripTrailingZeros().toPlainString()
        return canon.encodeToByteArray()
    }

    fun eqBoolean(b: Boolean): ByteArray = if (b) byteArrayOf(1) else byteArrayOf(0)
    fun eqNull(): ByteArray = byteArrayOf()

    fun foldedString(s: String): ByteArray = java.text.Normalizer
        .normalize(s, java.text.Normalizer.Form.NFKC)
        .lowercase()
        .encodeToByteArray()

    fun rangeNumberDouble(n: String): ByteArray {
        val d = n.toDouble()
        val bits = java.lang.Double.doubleToRawLongBits(d)
        val mask = if (bits ushr 63 == 1L) 0x7FFF_FFFF_FFFF_FFFFL else 0L
        val ord = bits xor mask
        return java.nio.ByteBuffer.allocate(8).putLong(ord).array()
    }

    fun rangeEpochMillis(millis: Long): ByteArray = java.nio.ByteBuffer.allocate(8).putLong(millis).array()
}

// --- Tokenizer ---
object Tokenizer {
    data class Out(val gin: MutableList<GinTerm>, val gpx: MutableList<GpxTerm>, val grg: MutableList<GrgTerm>)

    fun tokenize(json: JsonElement, cfg: IndexConfig): Out {
        val out = Out(mutableListOf(), mutableListOf(), mutableListOf())
        walk(json, emptyList(), cfg, out, 0)
        require(out.gin.size + out.gpx.size + out.grg.size <= cfg.maxTermsPerEvent) { "Too many index terms" }
        return out
    }

    private fun walk(node: JsonElement, path: List<String>, cfg: IndexConfig, out: Out, depth: Int) {
        require(depth <= cfg.maxDepth) { "Max JSON depth exceeded" }
        when (node) {
            is JsonObject -> {
                if (include(path, cfg)) out.gpx += GpxTerm(path)
                node.forEach { (k, v) -> walk(v, path + k, cfg, out, depth + 1) }
            }

            is JsonArray -> {
                if (include(path, cfg)) out.gpx += GpxTerm(path)
                node.forEach { v -> walk(v, path + WILDCARD, cfg, out, depth + 1) }
            }

            is JsonPrimitive -> if (include(path, cfg)) emitScalar(node, path, cfg, out)
        }
    }

    private fun include(path: List<String>, cfg: IndexConfig): Boolean {
        if (cfg.excludePaths.any { pathStartsWith(path, it) }) return false
        val inc = cfg.includePaths
        return inc == null || inc.any { pathStartsWith(path, it) }
    }

    private fun pathStartsWith(path: List<String>, prefix: List<String>): Boolean =
        path.size >= prefix.size && path.subList(0, prefix.size) == prefix

    private fun emitScalar(p: JsonPrimitive, path: List<String>, cfg: IndexConfig, out: Out) {
        when {
            p.isString -> {
                val s = p.content
                out.gin += GinTerm(TypeTag.STRING, path, Normalizers.eqString(s))
                if (cfg.foldCasePaths.contains(path)) {
                    out.gin += GinTerm(TypeTag.FOLDED_STRING, path, Normalizers.foldedString(s))
                }
            }

            p.booleanOrNull != null -> out.gin += GinTerm(TypeTag.BOOL, path, Normalizers.eqBoolean(p.boolean))
            p.longOrNull != null || p.doubleOrNull != null || p.content.toBigDecimalOrNull() != null -> {
                val canon = Normalizers.eqNumberCanonical(p.content)
                out.gin += GinTerm(TypeTag.NUMBER, path, canon)
                if (cfg.rangePaths.contains(path)) {
                    out.grg += GrgTerm(path, Normalizers.rangeNumberDouble(p.content))
                }
            }

            p.content.equals("null", ignoreCase = true) -> out.gin += GinTerm(TypeTag.NULL, path, Normalizers.eqNull())
            else -> { /* ignore exotic scalars */
            }
        }
    }
}

@ApplicationScoped
class OpenEventStoreFoundationDb(
    private val db: Database,
    private val tokenizer: EventTokenizer, // dependency to extract GIN/GPX/GRG terms
) {

    suspend fun storeEvent(
        tenant: ByteArray?,
        eventBytes: ByteArray,
        appendCondition: AppendCondition? = null,
    ): AppendResult {
        // Step 1: Tokenize event
        val (ginTerms, gpxTerms, grgTerms) = tokenizer.extract(eventBytes)

        // Step 2: Optionally enforce DCB consistency
        if (appendCondition != null) {
            enforceCondition(tenant, appendCondition)
        }

        // Step 3: Call the internal append function
        val versionstamp = appendInternal(tenant, eventBytes, ginTerms, gpxTerms, grgTerms)

        // Step 4: Return a structured result
        return AppendResult(versionstamp, eventBytes.size)
    }

    private suspend fun appendInternal(
        tenant: ByteArray?,
        eventBytes: ByteArray,
        gin: List<GinTerm>,
        gpx: List<GpxTerm>,
        grg: List<GrgTerm>,
    ): Versionstamp {
        return db.runAsync { tr ->
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, keyEv(tenant), eventBytes)

            gin.forEach { t ->
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, keyGin(tenant, t), ByteArray(0))
            }
            gpx.forEach { t ->
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, keyGpx(tenant, t), ByteArray(0))
            }
            grg.forEach { t ->
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, keyGrg(tenant, t), ByteArray(0))
            }

            tr.commit().thenApply { Versionstamp.fromBytes(tr.versionstamp.get()) }
        }.join()
    }

    private fun enforceCondition(tenant: ByteArray?, cond: AppendCondition) {
        // TODO: Implement dynamic consistency boundary check
        // Example: run query(cond.query), check last matching versionstamp == cond.expectedLastVersion
        // If mismatch -> throw AppendConditionFailedException
    }

    // Key builders (same as your code) ...

    fun keyEv(tenant: ByteArray?): ByteArray =
        Tuple.from("ev", tenant, Versionstamp.incomplete()).packWithVersionstamp()

    fun keyGin(tenant: ByteArray?, t: GinTerm): ByteArray =
        Tuple.from("gin", tenant, t.typeTag, *t.path.toTypedArray(), t.valueKey, Versionstamp.incomplete())
            .packWithVersionstamp()

    fun keyGpx(tenant: ByteArray?, t: GpxTerm): ByteArray =
        Tuple.from("gpx", tenant, *t.path.toTypedArray(), Versionstamp.incomplete()).packWithVersionstamp()

    fun keyGrg(tenant: ByteArray?, t: GrgTerm): ByteArray =
        Tuple.from("grg", tenant, *t.path.toTypedArray(), t.rangeCode, Versionstamp.incomplete()).packWithVersionstamp()
}

// Data classes for API
data class AppendCondition(
    val query: Query,
    val expectedLastVersion: Versionstamp,
)

data class AppendResult(
    val versionstamp: Versionstamp,
    val payloadSize: Int,
)

// Stub for tokenization
interface EventTokenizer {
    fun extract(eventBytes: ByteArray): Triple<List<GinTerm>, List<GpxTerm>, List<GrgTerm>>
      //  Tokenizer.tokenize()
}


data class AppendRequest(
    val events: List<Any>,
)

@Path("/test")
class MyHttpController(
    private val db: OpenEventStoreFoundationDb,
) {

    @POST
    fun test() {
        //    println("in test ${Thread.currentThread().name}")
        //  db.testBlocking()
    }

}