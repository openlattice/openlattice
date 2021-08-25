package com.openlattice

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityKeyGenerationBundle(
    val entitySetId: UUID,
    val keyPropertyTypeIds: Set<UUID>,
    val entities : List<Map<UUID, Set<Any>>>
)