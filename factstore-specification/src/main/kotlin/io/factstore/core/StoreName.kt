package io.factstore.core

@JvmInline
value class StoreName(val value: String) {

    companion object {
        const val MAX_LENGTH = 255
        const val REGEX_PATTERN = "^[a-zA-Z0-9_-]+$"
        private val regex = Regex(REGEX_PATTERN)
    }

    init {
        require(value.isNotBlank()) { "Name must not be empty or blank." }
        require(value.length <= MAX_LENGTH) { "Name must not exceed $MAX_LENGTH characters." }
        require(value.matches(regex)) { "Name must contain only alphanumeric characters, hyphens, and underscores." }
    }
}
