package com.openlattice.graph.processing

import com.openlattice.graph.EntityAggregationResult
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class NeighborhoodAggregationResult (
        val entityKeyId: UUID,
        val score: Double,
        val associationsEntityAggregationResult: EntityAggregationResult,
        val neighborsEntityAggregationResult: EntityAggregationResult
) {
}