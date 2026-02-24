package io.factstore.foundationdb

import com.apple.foundationdb.Database

class FdbFactStoreResetHelper(private val database: Database) {

    fun reset() {
        database.run { tr ->
            tr.clear(ByteArray(0), byteArrayOf(0xff.toByte()))
        }
    }
}