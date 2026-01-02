package org.factstore.core

import com.apple.foundationdb.Transaction
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    private val db = store.db

    override suspend fun append(fact: Fact) {
        db.runAsync { tr ->
            tr.store(fact)
            CompletableFuture.completedFuture(null)
        }.await()
    }

    override suspend fun append(facts: List<Fact>) {
        db.runAsync { tr ->
            facts.forEachIndexed { index, fact ->
                tr.store(fact, index)
            }
            CompletableFuture.completedFuture(null)
        }.await()
    }

    private fun Transaction.store(fact: Fact, index: Int = DEFAULT_INDEX) = with(store) {
        this@store.store(fact, index)
    }

}
