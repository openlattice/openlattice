package com.openlattice.graph.processing

import com.openlattice.graph.EntityAggregationResult
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class FilteredNeighborsRankingAggregationResult (
        val entityKeyId: UUID,
        val score: Double,
        val associationTypeId: UUID,
        val neighborTypeId: UUID,
        val associationsEntityAggregationResult: EntityAggregationResult,
        val neighborsEntityAggregationResult: EntityAggregationResult
)