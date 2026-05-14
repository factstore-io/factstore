package io.factstore.core

@JvmInline
value class StoreName(val value: String) {

    companion object {
        const val MAX_LENGTH = 255
        const val REGEX_PATTERN = "^[a-zA-Z]([a-zA-Z0-9_-]{0,253}[a-zA-Z0-9])?$|^[a-zA-Z]$"
        private val regex = Regex(REGEX_PATTERN)
    }

    init {
        require(value.length in 1..MAX_LENGTH) { "Name length must be between 1 and $MAX_LENGTH." }
        require(value.matches(regex)) {
            "Name must start with a letter and contain only alphanumeric characters, hyphens, or underscores."
        }
    }

    override fun toString(): String {
        return value
    }
}
