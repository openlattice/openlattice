package com.openlattice.graph

import com.google.common.base.Preconditions
import com.openlattice.data.DataEdgeKey
import com.openlattice.search.requests.EntityNeighborsFilter

private const val MAX_PAGE_SIZE = 10_000

/**
 * An object defining a request to load a page of neighbors
 *
 * @param filter A filter on the entityKeyIds to load neighbors for, as well ss optional filters on the source,
 * association, and destination entity set ids
 * @param bookmark The [DataEdgeKey] corresponding to the end of the previous page of search results. A null value will
 * signal to return the first page of results.
 * @param pageSize The number of neighbors to include in the response. A value of 0 will signal to return all neighbors.
 */
data class PagedNeighborRequest(
        val filter: EntityNeighborsFilter,
        val bookmark: DataEdgeKey? = null,
        val pageSize: Int = MAX_PAGE_SIZE
) {
    init {
        Preconditions.checkState(
                pageSize in 0..MAX_PAGE_SIZE,
                "pageSize must be between 0 and $MAX_PAGE_SIZE, inclusive."
        )
    }
}