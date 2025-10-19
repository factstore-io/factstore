package com.cassisi.openeventstore

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.FactStore
import com.cassisi.openeventstore.core.impl.ConditionalFdbFactAppender
import com.cassisi.openeventstore.core.impl.ConditionalTagQueryFdbFactAppender
import com.cassisi.openeventstore.core.impl.FdbFactAppender
import com.cassisi.openeventstore.core.impl.FdbFactFinder
import com.cassisi.openeventstore.core.impl.FdbFactStore
import com.cassisi.openeventstore.core.impl.FdbFactStreamer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces

class FdbConfig {

    @Produces
    @ApplicationScoped
    fun foundationDb(): Database {
        println("opening fdb cluster...")
        FDB.selectAPIVersion(730)
        val fdb = FDB.instance()
        val db = fdb.open("/etc/foundationdb/fdb.cluster")
        println("successfully opened...")
        return db
    }

    @Produces
    @ApplicationScoped
    fun factStore(db: Database): FactStore {
        val fdbFactStore = FdbFactStore(db)
        val factAppender = FdbFactAppender(fdbFactStore)
        val factFinder = FdbFactFinder(fdbFactStore)
        val factStreamer= FdbFactStreamer(fdbFactStore)
        val conditionalAppender = ConditionalFdbFactAppender(fdbFactStore)
        val conditionalTagQueryFdbFactAppender = ConditionalTagQueryFdbFactAppender(fdbFactStore)
        return FactStore(
            factAppender = factAppender,
            factFinder = factFinder,
            factStreamer = factStreamer,
            conditionalSubjectFactAppender = conditionalAppender,
            conditionalTagQueryFactAppender = conditionalTagQueryFdbFactAppender
        )
    }

}