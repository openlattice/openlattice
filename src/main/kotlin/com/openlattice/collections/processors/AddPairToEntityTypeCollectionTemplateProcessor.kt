package com.openlattice.collections.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.collection.CollectionTemplateType
import com.openlattice.edm.collection.EntityTypeCollection
import java.util.*

class AddPairToEntityTypeCollectionTemplateProcessor(
        val collectionTemplateType: CollectionTemplateType
) : AbstractRhizomeEntryProcessor<UUID, EntityTypeCollection, EntityTypeCollection?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntityTypeCollection>?): EntityTypeCollection? {
        val collection = entry?.value ?: return null

        collection.addTypeToTemplate(collectionTemplateType)
        entry.setValue(collection)

        return null
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AddPairToEntityTypeCollectionTemplateProcessor

        if (collectionTemplateType != other.collectionTemplateType) return false

        return true
    }

    override fun hashCode(): Int {
        return collectionTemplateType.hashCode()
    }


}