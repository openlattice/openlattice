package com.openlattice.linking

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class BlockingRequest(
        val entities: Map<UUID, Set<UUID>>,
        val blockSize: Int = 50
)