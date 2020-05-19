package com.openlattice.graph

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityAggregationResult(
        val scorable: Map<UUID, Double>,
        val passthrough: Map<UUID,Any?>
)