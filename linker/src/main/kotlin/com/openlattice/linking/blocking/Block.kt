package com.openlattice.linking.blocking

import com.openlattice.data.EntityDataKey
import java.util.UUID

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
data class Block(
        val entityDataKey: EntityDataKey,
        val entities: Map<EntityDataKey, Map<UUID, Set<Any>>>
)

