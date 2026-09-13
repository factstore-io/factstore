package io.factstore.core

/**
 * Requests the facts of a store that carry all of the given tags.
 *
 * @property storeName the store to read from
 * @property tags the tags a fact must carry; at least one, and at most
 *         [FactInput.MAX_TAGS], since no fact carries more
 * @property limit the maximum number of facts to return
 * @property direction the order in which facts are returned
 *
 * @throws IllegalArgumentException if [tags] is empty, or requires more tags
 *         than a fact can carry
 */
data class FindByTagsRequest(
    val storeName: StoreName,
    val tags: Map<TagKey, TagValue>,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
) {
    init {
        require(tags.isNotEmpty()) { "Tags must be defined!" }
        requireSatisfiableTagCount(tags)
    }
}
