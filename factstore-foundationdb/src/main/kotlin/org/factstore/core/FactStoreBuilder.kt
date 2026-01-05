package org.factstore.core

import com.apple.foundationdb.FDB

fun buildFdbFactStore(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    name: String = DEFAULT_FACT_STORE_NAME
): FactStore {
    val db = FDB.instance().open(clusterFilePath)
    val fdbFactStore = FdbFactStore(db, name)
    return FactStore(
        factAppender = FdbFactAppender(fdbFactStore),
        factFinder = FdbFactFinder(fdbFactStore),
        factStreamer = FdbFactStreamer(fdbFactStore),
    )
}
