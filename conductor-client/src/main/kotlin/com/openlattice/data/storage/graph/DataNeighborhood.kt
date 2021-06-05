package com.openlattice.data.storage.graph

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class DataNeighborhood(
        val entities: List<MutableMap<UUID,MutableSet<Any>>>
) {
}