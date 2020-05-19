package com.openlattice.graph

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AggregationResult(
        val rankings: SortedSet<NeighborhoodRankingAggregationResult>,
        val entities: Map<UUID,Map<UUID, Set<Any>>>,
        val associations: Map<UUID, Map<UUID, Set<Any>>>,
        val neighbors: Map<UUID, Map<UUID,Set<Any>>>
)