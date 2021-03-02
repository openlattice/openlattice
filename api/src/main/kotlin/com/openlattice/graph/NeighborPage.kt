package com.openlattice.graph

import com.openlattice.data.DataEdgeKey
import com.openlattice.data.requests.NeighborEntityDetails
import java.util.*

/**
 * A page of entity neighbors returned as the result of a submitted [PagedNeighborRequest].
 *
 * @param neighbors A map from entityKeyId -> list of entity neighbors
 * @param bookmark The final [DataEdgeKey] in the page. This value can be used to load the next page of neighbors.
 */
data class NeighborPage(
        val neighbors: Map<UUID, MutableList<NeighborEntityDetails>>,
        val bookmark: DataEdgeKey?
)