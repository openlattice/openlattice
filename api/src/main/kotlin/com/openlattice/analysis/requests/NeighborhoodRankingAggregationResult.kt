package com.openlattice.analysis.requests

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class NeighborhoodRankingAggregationResult(
        val entityKeyId: UUID,
        val score: Double,
        val results: List<FilteredNeighborsRankingAggregationResult>
) : Comparable<NeighborhoodRankingAggregationResult> {
    override fun compareTo(other: NeighborhoodRankingAggregationResult): Int {
        val result = score.compareTo(other.score)
        return if (result == 0) {
            entityKeyId.compareTo(other.entityKeyId)
        } else {
            result
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is NeighborhoodRankingAggregationResult) return false

        if (entityKeyId != other.entityKeyId) return false
        if (score != other.score) return false
        if (results != other.results) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entityKeyId.hashCode()
        result = 31 * result + score.hashCode()
        result = 31 * result + results.hashCode()
        return result
    }

    override fun toString(): String {
        return "NeighborhoodRankingAggregationResult(entityKeyId=$entityKeyId, score=$score, results=$results)"
    }

}