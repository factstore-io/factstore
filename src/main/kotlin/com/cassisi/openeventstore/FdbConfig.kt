package com.cassisi.openeventstore

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.dcb.FactStore
import com.cassisi.openeventstore.core.dcb.fdb.ConditionalFdbFactAppender
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactAppender
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactFinder
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactStore
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactStreamer
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
        return FactStore(factAppender, factFinder, factStreamer, conditionalAppender)
    }

}