package com.openlattice.linking.clustering

import com.openlattice.data.EntityDataKey
import com.openlattice.linking.BackgroundLinkingService
import com.openlattice.linking.BackgroundLinkingService.Companion.histogramify
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
            identifiedCluster: KeyedCluster
    ): ScoredCluster {
        val linkingEntities = BackgroundLinkingService.metrics.histogramify(
                BackgroundLinkingService::class.java,
                "cluster","loader","getLinkingEntities"
        ) { _, _ ->
             loader.getLinkingEntities(
                    BackgroundLinkingService.collectKeys(identifiedCluster.cluster) + blockKey
            )
        }

        val block = Block(
                blockKey,
                linkingEntities
        )
        //At some point, we may want to skip recomputing matches for existing cluster elements as an optimization.
        //Since we're freshly loading entities it's not too bad to recompute everything.

        val matchedBlock = BackgroundLinkingService.metrics.histogramify(
                BackgroundLinkingService::class.java,
                "cluster","matcher","match"
        ) { _, _ ->
            matcher.match(block)
        }
        val matchedCluster = Cluster(matchedBlock.matches)

        val score = BackgroundLinkingService.metrics.histogramify(
                BackgroundLinkingService::class.java,
                "cluster","scoreCluster"
        ) { _, _ ->
            scoreCluster(matchedCluster)
        }
        return ScoredCluster(identifiedCluster.id, matchedCluster, score)
    }
}
