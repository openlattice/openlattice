package com.openlattice.collections.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.collections.EntityTypeCollection
import com.openlattice.edm.requests.MetadataUpdate
import java.util.*

class UpdateEntityTypeCollectionMetadataProcessor(val update: MetadataUpdate):
        AbstractRhizomeEntryProcessor<UUID, EntityTypeCollection, EntityTypeCollection?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntityTypeCollection>): EntityTypeCollection? {
        val collection = entry.value

        if (update.title.isPresent) {
            collection.title = update.title.get()
        }

        if (update.description.isPresent) {
            collection.description = update.description.get()
        }

        if (update.type.isPresent) {
            collection.type = update.type.get()
        }

        entry.setValue(collection)
        return null
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateEntityTypeCollectionMetadataProcessor

        return update == other.update
    }

    override fun hashCode(): Int {
        return update.hashCode()
    }


}
