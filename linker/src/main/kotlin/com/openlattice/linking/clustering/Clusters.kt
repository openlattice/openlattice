package com.openlattice.linking.clustering

import com.openlattice.data.EntityDataKey
import java.util.UUID

/**
 * Wrapper for Map<EntityDataKey, Map<EntityDataKey, Double>>
 *
 * @author Drew Bailey (drew@openlattice.com)
 */
data class Cluster(val data: Map<EntityDataKey, Map<EntityDataKey, Double>> ):
        Map<EntityDataKey, Map<EntityDataKey, Double>> by data

/**
 * Wrapper for Pair<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>
 */
data class KeyedCluster(val id: UUID, val cluster: Cluster) {
    companion object {
        fun fromEntry( entry: Map.Entry<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>): KeyedCluster {
            return KeyedCluster(entry.key, Cluster(entry.value))
        }
    }
}

data class ScoredCluster(
        val clusterId: UUID,
        val cluster: Cluster,
        val score: Double
) : Comparable<Double> {
    override fun compareTo(other: Double): Int {
        return score.compareTo(other)
    }
}

/**
 * Wrapper for Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>
 */
data class Clusters(
        val data: Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>
): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>> by data {
    companion object {
        fun asClusters(
                clusters: Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>
        ): Set<KeyedCluster> {
            return clusters.entries.mapTo(mutableSetOf()) {
                KeyedCluster(it.key, Cluster(it.value))
            }
        }
    }
}
