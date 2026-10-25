package io.factstore.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.directory.DirectoryLayer
import io.factstore.core.FactStore
import kotlinx.coroutines.future.await

/** The directory FactStore keeps all of its keys under, created on first use. */
private const val ROOT_DIRECTORY = "factstore"

/**
 * Connects to a FoundationDB cluster and returns a [FactStore] on it.
 *
 * Every key lives under the `factstore` directory of the cluster, which is created if it does
 * not exist yet, so the cluster may be shared with other applications.
 *
 * FoundationDB fixes its API version once per process: calling this again with another
 * [apiVersion] fails, while calling it with the same one connects again.
 *
 * @param clusterFilePath the cluster file that says where the cluster is
 * @param apiVersion the FoundationDB API version to select
 * @param streamBatchSize the number of entries a stream reads per transaction; see [STREAM_BATCH_SIZE]
 */
suspend fun buildFdbFactStore(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    apiVersion: Int = 730,
    streamBatchSize: Int = STREAM_BATCH_SIZE,
): FactStore {
    FDB.selectAPIVersion(apiVersion)
    val db = FDB.instance().open(clusterFilePath)
    val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(ROOT_DIRECTORY)).await()
    val storage = FdbStorage(db, FdbKeyspace(root))
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
