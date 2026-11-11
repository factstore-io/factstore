package io.factstore.application

import io.factstore.core.FactId
import io.factstore.storage.LogCondition
import io.factstore.storage.LogQuery

interface SystemConsistencyBoundary<C> : ConsistencyBoundary<C, LogQuery, LogCondition, FactId> {

    override fun condition(command: C, query: LogQuery, version: FactId?): LogCondition =
        LogCondition.NoneMatch(query, after = version)
}
