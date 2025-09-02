package com.cassisi.openeventstore.core.dcb.fdb

class FdbFactStoreResetHelper(private val factStore: FdbFactStore) {

    fun reset() {
        factStore.db.run { tr ->
            val range = factStore.root.range()
            tr.clear(range.begin, range.end)
        }
    }
}