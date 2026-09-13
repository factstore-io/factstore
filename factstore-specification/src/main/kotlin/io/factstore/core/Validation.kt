package io.factstore.core

/**
 * The character set shared by every client-supplied text value of the fact
 * envelope — [Subject], [FactType], [TagKey], [TagValue], [MetadataKey] and
 * [MetadataValue].
 *
 * A conforming value starts and ends with a letter or digit and may otherwise
 * contain letters, digits and the separators `.`, `_`, `:`, `/` and `-`.
 */
const val CLEAN_TEXT_PATTERN = "^[A-Za-z0-9]([A-Za-z0-9._:/-]*[A-Za-z0-9])?$"

private val cleanTextRegex = Regex(CLEAN_TEXT_PATTERN)

/**
 * Validates a client-supplied envelope value against [CLEAN_TEXT_PATTERN] and
 * [maxLength].
 *
 * The length is checked first so that an oversized value is rejected without
 * running the pattern over it.
 *
 * @param value the value to validate
 * @param field the name of the validated value, used in the failure message
 * @param maxLength the maximum permitted number of characters
 * @param allowEmpty whether the empty string is accepted, used by the value
 *        types that give it presence-only meaning
 * @throws IllegalArgumentException if [value] is too long or does not conform
 */
internal fun requireCleanText(
    value: String,
    field: String,
    maxLength: Int,
    allowEmpty: Boolean = false,
) {
    if (allowEmpty && value.isEmpty()) return

    require(value.length <= maxLength) {
        "$field must not exceed $maxLength characters, but was ${value.length}."
    }
    require(cleanTextRegex.matches(value)) {
        "$field must start and end with a letter or digit and may otherwise contain only " +
                "letters, digits, '.', '_', ':', '/' and '-', but was '$value'."
    }
}

/**
 * The size of this string in bytes when encoded as UTF-8.
 */
internal val String.utf8Size: Int
    get() = encodeToByteArray().size
