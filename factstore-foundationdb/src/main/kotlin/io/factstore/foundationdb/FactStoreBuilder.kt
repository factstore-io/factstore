package io.factstore.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.directory.DirectoryLayer
import io.factstore.core.FactStore
import kotlinx.coroutines.future.await

/**
 * @param streamBatchSize the number of entries a stream reads per transaction; see [STREAM_BATCH_SIZE]
 */
suspend fun buildFdbFactStore(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    apiVersion: Int = 730,
    streamBatchSize: Int = STREAM_BATCH_SIZE,
): FactStore {
    FDB.selectAPIVersion(apiVersion)
    val db = FDB.instance().open(clusterFilePath)
    val root = DirectoryLayer.getDefault().createOrOpen(db, listOf("factstore")).await()
    val storage = FdbStorage(db, FdbKeyspace.under(root))
    return FactStore(
        factAppender = FdbFactAppender(storage),
        factFinder = FdbFactFinder(storage),
        factStreamer = FdbFactStreamer(storage, streamBatchSize = streamBatchSize),
        factSubscriber = FdbFactSubscriber(storage),
        storeFactory = FdbStoreFactory(storage),
        storeFinder = FdbStoreFinder(storage),
        storeRemover = FdbStoreRemover(storage),
    )
}

