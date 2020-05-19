package com.openlattice.analysis

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