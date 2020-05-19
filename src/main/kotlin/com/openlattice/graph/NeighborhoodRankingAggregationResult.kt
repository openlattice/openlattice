package com.openlattice.graph

import com.openlattice.graph.processing.FilteredNeighborsRankingAggregationResult

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class NeighborhoodRankingAggregationResult(
        val score: Double,
        val results: List<FilteredNeighborsRankingAggregationResult>
) : Comparable<NeighborhoodRankingAggregationResult> {
    override fun compareTo(other: NeighborhoodRankingAggregationResult): Int = score.compareTo(other.score)
}