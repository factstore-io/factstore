package io.factstore.core

/**
 * The name a store is addressed by, unique among the stores of a FactStore.
 *
 * A name starts with a letter, ends with a letter or digit, and may otherwise contain letters,
 * digits, `-` and `_`: it appears in URLs and on command lines, so it is kept to characters that
 * need no escaping in either.
 *
 * Unlike the other value types, a store name renders as its plain [value], since it is quoted in
 * messages and logs far more often than it is inspected.
 *
 * @property value the name; must conform to [REGEX_PATTERN] and must not exceed [MAX_LENGTH]
 *         characters
 *
 * @throws IllegalArgumentException if [value] is empty, too long, or does not conform to
 *         [REGEX_PATTERN]
 */
@JvmInline
value class StoreName(val value: String) {

    companion object {

        /** The maximum length of a store name. */
        const val MAX_LENGTH = 256

        /** The characters a store name may consist of; its length is checked against [MAX_LENGTH]. */
        const val REGEX_PATTERN = "^[a-zA-Z]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$"

        private val regex = Regex(REGEX_PATTERN)
    }

    init {
        require(value.length <= MAX_LENGTH) {
            "A store name must not exceed $MAX_LENGTH characters, but was ${value.length}."
        }
        require(regex.matches(value)) {
            "A store name must start with a letter, end with a letter or digit, and may otherwise " +
                    "contain only letters, digits, '-' and '_', but was '$value'."
        }
    }

    override fun toString(): String = value
}

fun String.toStoreName() = StoreName(this)
