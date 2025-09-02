package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple

const val FACT_STORE = "fact-store"
const val FACT_ID = "id"
const val FACT_TYPE = "type"
const val FACT_PAYLOAD = "payload"
const val CREATED_AT = "created-at"

const val GLOBAL_FACT_POSITION_INDEX = "global"
const val CREATED_AT_INDEX = "created-at-index"
const val EVENT_TYPE_INDEX = "type-index"

val EMPTY_BYTE_ARRAY = ByteArray(0)
const val DEFAULT_INDEX = 0

/**
 * A simple event/fact store implementation based on FoundationDB.
 *
 * FACT SPACES:
 *  /fact-store/id/{factId} = ∅   (existence / deduplication anchor)
 *  /fact-store/type/{factId} = type
 *  /fact-store/payload/{factId} = payload
 *  /fact-store/created-at/{factId} = timestamp in UTC
 *
 * INDEX SPACES
 *  /fact-store/global/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/type-index/{type}/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/created-at-index/{epochSecond}/{nano}/{vs}/{index}/{factId} = ∅
 *
 */
class FdbFactStore(
    internal val db: Database
) {


    // DIRECTORIES
    internal val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(FACT_STORE)).get()

    // FACT SPACES
    internal val factIdSubspace = root.subspace(Tuple.from(FACT_ID))
    internal val factTypeSubspace = root.subspace(Tuple.from(FACT_TYPE))
    internal val factPayloadSubspace = root.subspace(Tuple.from(FACT_PAYLOAD))
    internal val createdAtSubspace = root.subspace(Tuple.from(CREATED_AT))

    // INDEX SPACES
    internal val globalFactPositionSubspace = root.subspace(Tuple.from(GLOBAL_FACT_POSITION_INDEX))
    internal val eventTypeIndexSubspace = root.subspace(Tuple.from(EVENT_TYPE_INDEX))
    internal val createdAtIndexSubspace = root.subspace(Tuple.from(CREATED_AT_INDEX))

}