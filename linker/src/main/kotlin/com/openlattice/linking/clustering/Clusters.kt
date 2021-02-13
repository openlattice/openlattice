package com.openlattice.linking.clustering

import com.openlattice.data.EntityDataKey
import java.util.UUID

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
data class Clusters(
    val data: Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>
): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>> by data {
    companion object {
        fun asClusters(
                clusters: Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>
        ): Set<Cluster> {
            return clusters.entries.mapTo(mutableSetOf()) {
                Cluster(it.key, it.value)
            }
        }
    }
}

data class Cluster( val id: UUID, val data: Map<EntityDataKey, Map<EntityDataKey, Double>> ) {
    companion object {
        fun fromEntry( entry: Map.Entry<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>): Cluster {
            return Cluster(entry.key, entry.value)
        }
    }
}