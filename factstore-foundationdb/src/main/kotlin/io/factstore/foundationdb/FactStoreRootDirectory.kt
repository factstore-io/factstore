package io.factstore.foundationdb

import com.apple.foundationdb.directory.DirectorySubspace

internal const val STORES = "stores"
internal const val STORE_INDEX = "store-index"

data class FactStoreRootDirectory(
    val rootDirectorySubspace: DirectorySubspace
)
