package io.factstore.core

/**
 * Base exception type for all FactStoreFactory-related errors.
 *
 * @author Domenic Cassisi
 */
sealed class FactStoreFactoryException(message: String) : FactStoreException(message)

/**
 * Thrown when attempting to create a fact store with a name that already exists.
 *
 * @property name the name of the fact store that already exists
 */
class FactStoreAlreadyExistsException(val name: String) :
    FactStoreFactoryException("FactStore with name '$name' already exists")

/**
 * Thrown when attempting to access or delete a fact store that does not exist.
 *
 * @property name the name of the fact store that was not found
 */
class FactStoreNotFoundException(val name: String) :
    FactStoreFactoryException("FactStore with name '$name' not found")

/**
 * Thrown when a fact store name does not meet validation requirements.
 *
 * Valid names must consist only of alphanumeric characters, hyphens, and underscores
 * (URL-safe characters).
 *
 * @property name the invalid name
 * @property reason detailed explanation of why the name is invalid
 */
class InvalidFactStoreNameException(val name: String, val reason: String) :
    FactStoreFactoryException("Invalid fact store name '$name': $reason")
