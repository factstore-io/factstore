package io.factstore.client.exceptions

sealed class FactStoreException(message: String) : Exception(message)

class StoreNotFoundException(val storeName: String) :
    FactStoreException("Store '$storeName' not found")

class StoreNameAlreadyExistsException(val storeName: String) :
    FactStoreException("A store named '$storeName' already exists")

class FactNotFoundException(val factId: String) :
    FactStoreException("Fact '$factId' not found")

class AppendConditionViolatedException :
    FactStoreException("Append condition was violated")

class DuplicateFactIdsException(val factIds: List<String>) :
    FactStoreException("Duplicate fact IDs: ${factIds.joinToString()}")
