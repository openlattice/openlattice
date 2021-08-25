package com.openlattice.linking.matching

import com.openlattice.data.EntityDataKey

/**
 *
 * Wrapper for Pair<EntityDataKey, MutableMap<EntityDataKey, MutableMap<EntityDataKey, Double>>>
 *
 * @author Drew Bailey (drew@openlattice.com)
 */
data class PairwiseMatch(
        val candidate: EntityDataKey,
        val matches: MutableMap<EntityDataKey, MutableMap<EntityDataKey, Double>>
)