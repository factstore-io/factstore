package io.factstore.core

data class FindBySubjectRequest(
    val storeName: StoreName,
    val subject: Subject,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
)
