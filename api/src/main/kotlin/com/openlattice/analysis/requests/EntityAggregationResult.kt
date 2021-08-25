package com.openlattice.analysis.requests

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityAggregationResult(
        val scorable: Map<UUID, Double>,
        val comparable: Map<UUID,Comparable<*>>
)