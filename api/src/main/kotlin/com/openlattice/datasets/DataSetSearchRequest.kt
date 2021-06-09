package com.openlattice.datasets

import java.util.*

data class DataSetSearchRequest(
        val searchTerm: String,
        val start: Int = 0,
        val maxHits: Int = 10_000,
        val organizationIds: Set<UUID>? = null,
        val excludeColumns: Boolean = false
)
