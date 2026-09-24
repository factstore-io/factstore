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
    val rootDir = DirectoryLayer.getDefault().createOrOpen(db, listOf("factstore")).await()
    val rootDirectory = FactStoreRootDirectory(rootDir)
    val context = FdbFactStoreContext.create(rootDirectory)
    val fdbFactStore = FdbFactStore(db, context)
    val streamer = FdbFactStreamer(fdbFactStore, streamBatchSize = streamBatchSize)
    return FactStore(
        factAppender = FdbFactAppender(fdbFactStore),
        factFinder = FdbFactFinder(fdbFactStore),
        factStreamer = streamer,
        factSubscriber = streamer,
        storeFactory = FdbStoreFactory(fdbFactStore),
        storeFinder = FdbStoreFinder(fdbFactStore),
        storeRemover = FdbStoreRemover(fdbFactStore),
    )
}

