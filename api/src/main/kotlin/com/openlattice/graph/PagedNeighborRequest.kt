package com.openlattice.graph

import com.google.common.base.Preconditions
import com.openlattice.data.DataEdgeKey
import com.openlattice.search.requests.EntityNeighborsFilter

private const val MAX_PAGE_SIZE = 10_000

data class PagedNeighborRequest(
        val filter: EntityNeighborsFilter,
        val bookmark: DataEdgeKey? = null,
        val pageSize: Int = MAX_PAGE_SIZE
) {
    init {
        Preconditions.checkState(
                pageSize in 1..MAX_PAGE_SIZE,
                "pageSize must be between 1 and $MAX_PAGE_SIZE, inclusive."
        )
    }
}