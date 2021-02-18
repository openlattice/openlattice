package com.openlattice.linking.clustering

import com.openlattice.data.EntityDataKey
import com.openlattice.linking.BackgroundLinkingService
import com.openlattice.linking.DataLoader
import com.openlattice.linking.blocking.Block
import com.openlattice.linking.matching.Matcher

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class PostgresClusterer(
        private val loader: DataLoader,
        private val matcher: Matcher
): Clusterer {

    companion object {
        private fun scoreCluster( matchedCluster: Cluster ): Double {
            return matchedCluster.values.flatMap { it.values }.min() ?: 0.0
        }
    }

    override fun cluster(
            blockKey: EntityDataKey,
            identifiedCluster: KeyedCluster,
            clusteringStrategy: (Cluster) -> Double
    ): ScoredCluster {
        val block = Block(blockKey, loader.getEntities(
                BackgroundLinkingService.collectKeys(identifiedCluster.cluster) + blockKey)
        )
        //At some point, we may want to skip recomputing matches for existing cluster elements as an optimization.
        //Since we're freshly loading entities it's not too bad to recompute everything.
        val matchedBlock = matcher.match(block)
        val matchedCluster = Cluster(matchedBlock.matches)
        val score = scoreCluster(matchedCluster)
        return ScoredCluster(identifiedCluster.id, matchedCluster, score)
    }
}
