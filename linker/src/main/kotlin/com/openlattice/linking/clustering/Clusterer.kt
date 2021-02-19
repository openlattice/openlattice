package com.openlattice.linking.clustering

import com.openlattice.data.EntityDataKey

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
interface Clusterer {
    fun cluster(
            blockKey: EntityDataKey,
            identifiedCluster: KeyedCluster
    ): ScoredCluster
}