package io.factstore.core

sealed interface ExistsByIdResult {
    data object Exists: ExistsByIdResult
    data object DoesNotExist: ExistsByIdResult
    data object FactstoreNotFound: ExistsByIdResult
}