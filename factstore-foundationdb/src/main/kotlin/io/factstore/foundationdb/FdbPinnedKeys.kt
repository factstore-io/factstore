package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import io.factstore.core.ReadDirection
import io.factstore.core.FactType
import io.factstore.core.StoreId
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue

/**
 * The keys of [range] up to and including [pinnedEndKey], read in [direction].
 *
 * A read starts at one end and continues from its cursor, the last key read, towards the
 * other end: reading forward moves the begin past the cursor, reading backward moves the end
 * before it.
 */
internal class PinnedKeys(range: Range, pinnedEndKey: ByteArray, direction: ReadDirection) {

    val reverse = direction.isReverse()

    private val first = KeySelector.firstGreaterOrEqual(range.begin)

    // An exclusive end that includes the pinned key itself.
    private val last = KeySelector.firstGreaterThan(pinnedEndKey)

    /** The begin and end of the keys still to be read after [cursor]; all of them if it is `null`. */
    fun remainingAfter(cursor: ByteArray?): Pair<KeySelector, KeySelector> = when {
        cursor == null -> first to last
        reverse -> first to KeySelector.firstGreaterOrEqual(cursor)
        else -> KeySelector.firstGreaterThan(cursor) to last
    }

    /** The begin and end of the keys from [key] onwards, [key] included. */
    fun remainingFrom(key: ByteArray): Pair<KeySelector, KeySelector> =
        if (reverse) first to KeySelector.firstGreaterThan(key)
        else KeySelector.firstGreaterOrEqual(key) to last

    /** Orders positions the way this scan reads them. */
    val readingOrder: Comparator<FactPosition> = direction.toComparator()
}

/** The keys of one subject's index in this store, up to the fact at [head]. */
internal fun SubjectIndexSubspace.pinnedKeys(
    storeId: StoreId,
    subject: Subject,
    head: FactPosition,
    direction: ReadDirection,
): PinnedKeys = PinnedKeys(
    range = range(storeId, subject),
    pinnedEndKey = getKey(storeId, subject, head),
    direction = direction,
)

/** The keys of one type's index in this store, up to the fact at [head]. */
internal fun EventTypeIndexSubspace.pinnedKeys(
    storeId: StoreId,
    type: FactType,
    head: FactPosition,
    direction: ReadDirection,
): PinnedKeys = PinnedKeys(
    range = range(storeId, type),
    pinnedEndKey = getKey(storeId, type, head),
    direction = direction,
)

/** The keys of one type-and-tag combination's index in this store, up to the fact at [head]. */
internal fun TagsTypeIndexSubspace.pinnedKeys(
    storeId: StoreId,
    type: FactType,
    tag: Pair<TagKey, TagValue>,
    head: FactPosition,
    direction: ReadDirection,
): PinnedKeys = PinnedKeys(
    range = range(storeId, type, tag),
    pinnedEndKey = getKey(storeId, type, tag, head),
    direction = direction,
)

/** The keys of one tag's index in this store, up to the fact at [head]. */
internal fun TagsIndexSubspace.pinnedKeys(
    storeId: StoreId,
    tag: Pair<TagKey, TagValue>,
    head: FactPosition,
    direction: ReadDirection,
): PinnedKeys = PinnedKeys(
    range = range(storeId, tag),
    pinnedEndKey = getKey(storeId, tag, head),
    direction = direction,
)
