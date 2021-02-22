package com.openlattice.linking.blocking

import com.openlattice.data.EntityDataKey
import java.util.UUID

/**
 * @author Drew Bailey (drew@openlattice.com)
 * Wrapper for Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>
 */
data class Block(
        val entityDataKey: EntityDataKey,
        val entities: Map<EntityDataKey, Map<UUID, Set<Any>>>
) {
    val size = entities.values.map { it.size }.sum()
}

