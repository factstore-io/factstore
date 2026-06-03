package io.factstore.core

data class FindInTimeRangeRequest(
    val storeName: StoreName,
    val timeRange: TimeRange,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
)
