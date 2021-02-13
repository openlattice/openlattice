package com.openlattice.linking.clustering

import com.openlattice.data.EntityDataKey
import com.openlattice.linking.ScoredCluster

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
interface Clusterer {
    fun cluster(
            blockKey: EntityDataKey,
            identifiedCluster: Cluster,
            clusteringStrategy: (Map<EntityDataKey, Map<EntityDataKey, Double>>) -> Double
    ): ScoredCluster
}