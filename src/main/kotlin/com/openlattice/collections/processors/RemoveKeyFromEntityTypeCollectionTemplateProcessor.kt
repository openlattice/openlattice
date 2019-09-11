package com.openlattice.collections.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.collections.EntityTypeCollection
import java.util.*

class RemoveKeyFromEntityTypeCollectionTemplateProcessor(
        val templateTypeId: UUID
) : AbstractRhizomeEntryProcessor<UUID, EntityTypeCollection, EntityTypeCollection?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntityTypeCollection>): EntityTypeCollection? {
        val collection = entry.value

        collection.removeTemplateTypeFromTemplate(templateTypeId)
        entry.setValue(collection)

        return null
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RemoveKeyFromEntityTypeCollectionTemplateProcessor

        if (templateTypeId != other.templateTypeId) return false

        return true
    }

    override fun hashCode(): Int {
        return templateTypeId.hashCode()
    }


}