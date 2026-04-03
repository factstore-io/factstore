package io.factstore.foundationdb

import com.apple.foundationdb.directory.DirectorySubspace
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple

const val STORES = "stores"
const val STORE_INDEX = "store-index"

data class FactStoreRootDirectory(
    val rootDirectorySubspace: DirectorySubspace
)
