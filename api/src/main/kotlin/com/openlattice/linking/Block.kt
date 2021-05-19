package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 * Wrapper for Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>
 */
data class Block(
        val entityDataKey: EntityDataKey,
        val entities: Map<EntityDataKey, Map<UUID, Set<Any>>>
) {
    val size = entities.values.map { it.size }.sum()

    companion object {
        fun emptyBlock(edk: EntityDataKey): Block {
            return Block(edk, mapOf(edk to mapOf()))
        }
    }

}
