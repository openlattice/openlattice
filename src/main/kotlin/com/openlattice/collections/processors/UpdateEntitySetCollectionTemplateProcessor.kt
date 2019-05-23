package com.openlattice.collections.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.collection.EntitySetCollection
import java.util.*

class UpdateEntitySetCollectionTemplateProcessor(val template: Map<UUID, UUID>) : AbstractRhizomeEntryProcessor<UUID, EntitySetCollection, EntitySetCollection?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySetCollection>?): EntitySetCollection? {
        val collection = entry?.value ?: return null

        collection.template = template
        entry.setValue(collection)

        return null
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateEntitySetCollectionTemplateProcessor

        if (template != other.template) return false

        return true
    }

    override fun hashCode(): Int {
        return template.hashCode()
    }


}