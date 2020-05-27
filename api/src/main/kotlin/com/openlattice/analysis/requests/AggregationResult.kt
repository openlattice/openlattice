package com.openlattice.analysis.requests

import com.openlattice.analysis.requests.NeighborhoodRankingAggregationResult
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AggregationResult(
        val rankings: SortedSet<NeighborhoodRankingAggregationResult>,
        val entities: Map<UUID, Map<FullQualifiedName, Set<Any>>>,
        val edges: Map<UUID, Map<UUID, UUID>>
)