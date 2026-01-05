package org.factstore.core

sealed interface AppendResult {

    data object Appended : AppendResult

    data object AlreadyApplied : AppendResult

    data object AppendConditionViolated : AppendResult

}
