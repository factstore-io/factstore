package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import io.factstore.core.FactStore

suspend fun buildFdbFactStore(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    name: String = DEFAULT_FACT_STORE_NAME,
    apiVersion: Int = 730
): FactStore {
    FDB.selectAPIVersion(apiVersion)
    val db = FDB.instance().open(clusterFilePath)
    val context = FdbFactStoreContext.create(db, name)
    val fdbFactStore = FdbFactStore(db, context)
    return FactStore(
        factAppender = FdbFactAppender(fdbFactStore),
        factFinder = FdbFactFinder(fdbFactStore),
        factStreamer = FdbFactStreamer(fdbFactStore),
    )
}

fun initDatabase(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    apiVersion: Int = 730
): FoundationDBFactStoreContext {
    FDB.selectAPIVersion(apiVersion)
    val db = FDB.instance().open(clusterFilePath)
    return FoundationDBFactStoreContext(database = db)
}

data class FoundationDBFactStoreContext(
    val database: Database,
)

suspend fun buildFdbFactStore(
    context: FoundationDBFactStoreContext,
    name: String
): FactStore {
    val fdbFactStore = FdbFactStore(
        context.database,
        FdbFactStoreContext.create(context.database, name))

    return FactStore(
        factAppender = FdbFactAppender(fdbFactStore),
        factFinder = FdbFactFinder(fdbFactStore),
        factStreamer = FdbFactStreamer(fdbFactStore),
    )
}
