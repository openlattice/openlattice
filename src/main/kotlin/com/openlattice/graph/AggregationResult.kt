package com.openlattice.graph

import com.openlattice.graph.processing.NeighborhoodAggregationResult
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AggregationResult(
        val rankings: List<NeighborhoodAggregationResult>,
        val entities: Map<UUID,Map<UUID, Set<Any>>>,
        val associations: Map<UUID, Map<UUID, Set<Any>>>,
        val neighbors: Map<UUID, Map<UUID,Set<Any>>>,
        val associationScores: Map<UUID, Double>,
        val neighborScores: Map<UUID, Double>,
        val associationUnscored: Map<UUID, Comparable<*>>,
        val neighborUnscored: Map<UUID, Comparable<*>>,
        val score: Double
)