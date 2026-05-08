package io.factstore.foundationdb

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
        storeFactory = FdbStoreFactory(fdbFactStore),
        storeFinder = FdbStoreFinder(fdbFactStore),
        storeRemover = FdbStoreRemover(fdbFactStore),
    )
}

