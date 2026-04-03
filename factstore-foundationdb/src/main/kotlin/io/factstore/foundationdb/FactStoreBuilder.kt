package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.apple.foundationdb.directory.DirectoryLayer
import io.factstore.core.FactStore
import kotlinx.coroutines.future.await

suspend fun buildFdbFactStore(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    apiVersion: Int = 730
): FactStore {
    FDB.selectAPIVersion(apiVersion)
    val db = FDB.instance().open(clusterFilePath)
    val rootDir = DirectoryLayer.getDefault().createOrOpen(db, listOf("factstore")).await()
    val rootDirectory = FactStoreRootDirectory(rootDir)
    val context = FdbFactStoreContext.create(rootDirectory)
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
