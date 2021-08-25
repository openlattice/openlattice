package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

/**
 * @author Matthew Tamayo-Rios (matthew@openlattice.com)
 * Used to return the results of a [BlockingRequest].
 */
data class BlockedEntity(
        val entityDataKey: EntityDataKey,
        val properties: Map<FullQualifiedName, Set<Any>>
)