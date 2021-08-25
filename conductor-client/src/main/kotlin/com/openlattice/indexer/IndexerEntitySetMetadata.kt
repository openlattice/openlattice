package com.openlattice.indexer

import com.openlattice.edm.EntitySet
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class IndexerEntitySetMetadata private constructor(
        val id: UUID,
        val entityTypeId: UUID,
        var name: String,
        val partitions: Array<Int>
) {
    companion object {
        fun fromEntitySet( es: EntitySet ): IndexerEntitySetMetadata {
            return IndexerEntitySetMetadata(
                    es.id,
                    es.entityTypeId,
                    es.name,
                    es.partitions.toTypedArray()
            )
        }
    }
}