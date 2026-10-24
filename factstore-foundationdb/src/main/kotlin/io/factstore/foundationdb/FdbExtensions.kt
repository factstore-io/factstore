package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import io.factstore.core.ReadDirection
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.supervisorScope

/**
 * Converts a [ReadDirection] to the reverse boolean expected by FDB's getRange.
 */
fun ReadDirection.isReverse(): Boolean = this == ReadDirection.Backward

/**
 * Comparator for [FactPosition] respecting [ReadDirection].
 * Used when direction must be applied in application code (e.g. after intersection).
 */
fun ReadDirection.toComparator(): Comparator<FactPosition> = when (this) {
    ReadDirection.Forward -> compareBy { it }
    ReadDirection.Backward -> compareByDescending { it }
}

/**
 * Runs [block] in a read transaction, which FoundationDB retries as a whole on a retryable error.
 *
 * The block is a coroutine of the caller, so it keeps the caller's context and is cancelled with
 * it. It runs in a supervisor scope: a failed attempt completes its future exceptionally, for
 * FoundationDB to retry, rather than cancelling the caller. Like any
 * transaction body it may run more than once, so it must not act outside the transaction.
 */
internal suspend fun <T> Database.inReadTransaction(block: suspend ReadTransaction.() -> T): T =
    supervisorScope { readAsync { tr -> future { tr.block() } }.await() }

/** Runs [block] in a transaction that may write, committed when it returns; see [inReadTransaction]. */
internal suspend fun <T> Database.inTransaction(block: suspend Transaction.() -> T): T =
    supervisorScope { runAsync { tr -> future { tr.block() } }.await() }
