package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import java.util.UUID

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
data class ScoredCluster(
        val clusterId: UUID,
        val cluster: Map<EntityDataKey, Map<EntityDataKey, Double>>,
        val score: Double
) : Comparable<Double> {
    override fun compareTo(other: Double): Int {
        return score.compareTo(other)
    }
}